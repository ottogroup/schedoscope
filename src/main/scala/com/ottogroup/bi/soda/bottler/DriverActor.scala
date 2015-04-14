package com.ottogroup.bi.soda.bottler

import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import com.ottogroup.bi.soda.DriverSettings
import com.ottogroup.bi.soda.Settings
import com.ottogroup.bi.soda.bottler.driver.Driver
import com.ottogroup.bi.soda.bottler.driver.DriverException
import com.ottogroup.bi.soda.bottler.driver.DriverRunFailed
import com.ottogroup.bi.soda.bottler.driver.DriverRunHandle
import com.ottogroup.bi.soda.bottler.driver.DriverRunOngoing
import com.ottogroup.bi.soda.bottler.driver.DriverRunState
import com.ottogroup.bi.soda.bottler.driver.DriverRunSucceeded
import com.ottogroup.bi.soda.bottler.driver.FileSystemDriver
import com.ottogroup.bi.soda.bottler.driver.HiveDriver
import com.ottogroup.bi.soda.bottler.driver.OozieDriver
import com.ottogroup.bi.soda.dsl.Transformation
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.event.Logging
import akka.event.LoggingReceive
import com.ottogroup.bi.soda.dsl.transformations.HiveTransformation
import com.ottogroup.bi.soda.dsl.transformations.FilesystemTransformation
import com.ottogroup.bi.soda.dsl.transformations.OozieTransformation
import com.ottogroup.bi.soda.dsl.transformations.MorphlineTransformation
import com.ottogroup.bi.soda.bottler.driver.MorphlineDriver

class DriverActor[T <: Transformation](actionsManagerActor: ActorRef, ds: DriverSettings, driverConstructor: (DriverSettings) => Driver[T], pingDuration: FiniteDuration) extends Actor {
  import context._
  val log = Logging(system, this)

  lazy val driver = driverConstructor(ds)

  var runningCommand: Option[CommandWithSender] = None

  override def preStart() {
    logStateInfo("idle", "DRIVER ACTOR: initialized actor")
    tick()
  }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    if (runningCommand.isDefined)
      actionsManagerActor ! runningCommand.get
  }

  def tick() {
    system.scheduler.scheduleOnce(pingDuration, self, "tick")
  }

  def receive = LoggingReceive {
    case CommandWithSender(command, sender) => toRunning(CommandWithSender(command, sender))

    case "tick" => {
      actionsManagerActor ! PollCommand(driver.transformationName)
      tick()
    }
  }

  def running(runHandle: DriverRunHandle[T], s: ActorRef): Receive = LoggingReceive {
    case KillAction() => {
      driver.killRun(runHandle)
      toReceive()
    }

    case c: CommandWithSender => actionsManagerActor ! c

    case "tick" => try {
      driver.getDriverRunState(runHandle) match {
        case _: DriverRunOngoing[T] => tick()

        case success: DriverRunSucceeded[T] => {
          log.info(s"DRIVER ACTOR: Driver run for handle=${runHandle} succeeded.")
          s ! ActionSuccess(runHandle, success)
          toReceive()
          tick()
        }

        case failure: DriverRunFailed[T] => {
          log.error(s"DRIVER ACTOR: Driver run for handle=${runHandle} failed. ${failure.reason}, cause ${failure.cause}")
          s ! ActionFailure(runHandle, failure)
          toReceive()
          tick()
        }
      }
    } catch {
      case exception: DriverException => {
        log.error(s"DRIVER ACTOR: Driver exception caught by driver actor in running state, rethrowing: ${exception.message}, cause ${exception.cause}")
        throw exception
      }

      case t: Throwable => {
        log.error(s"DRIVER ACTOR: Unexpected exception caught by driver actor in running state, rethrowing: ${t.getMessage()}, cause ${t.getCause()}")
        throw t
      }
    }
  }

  def toReceive() {
    runningCommand = None

    logStateInfo("idle", "DRIVER ACTOR: becoming idle")

    unbecome()
    become(receive)
  }

  def toRunning(commandToRun: CommandWithSender) {
    runningCommand = Some(commandToRun)

    try {
      if (commandToRun.command.isInstanceOf[Deploy]) {

        logStateInfo("deploy", s"DRIVER ACTOR: Running Deploy command")

        driver.deployAll(ds)
        commandToRun.sender ! DeployActionSuccess()

        logStateInfo("idle", "DRIVER ACTOR: becoming idle")

        runningCommand = None
      } else {
        val runHandle = driver.run(commandToRun.command.asInstanceOf[T])

        logStateInfo("running", s"DRIVER ACTOR: Running command ${commandToRun}, runHandle=${runHandle}", runHandle, driver.getDriverRunState(runHandle))

        unbecome()
        become(running(runHandle, commandToRun.sender))
      }
    } catch {
      case exception: DriverException => {
        log.error(s"DRIVER ACTOR: Driver exception caught by driver actor in receive state, rethrowing: ${exception.message}, cause ${exception.cause}")
        throw exception
      }

      case t: Throwable => {
        log.error(s"DRIVER ACTOR: Unexpected exception caught by driver actor in receive state, rethrowing: ${t.getMessage()}, cause ${t.getCause()}")
        throw t
      }
    }
  }

  def logStateInfo(state: String, message: String, runHandle: DriverRunHandle[T] = null, runState: DriverRunState[T] = null) {
    actionsManagerActor ! ActionStatusResponse(state, self, driver, runHandle, runState)
    log.info(message)
  }
}

object DriverActor {
  def props(driverName: String, actionsRouter: ActorRef) = {
    val ds = Settings().getDriverSettings(driverName)

    driverName match {
      case "hive" => Props(
        classOf[DriverActor[HiveTransformation]],
        actionsRouter, ds, (d: DriverSettings) => HiveDriver(d), 5 seconds).withDispatcher("akka.actor.views-dispatcher")

      case "filesystem" => Props(
        classOf[DriverActor[FilesystemTransformation]],
        actionsRouter, ds, (d: DriverSettings) => FileSystemDriver(d), 100 milliseconds).withDispatcher("akka.actor.views-dispatcher")

      case "oozie" => Props(
        classOf[DriverActor[OozieTransformation]],
        actionsRouter, ds, (d: DriverSettings) => OozieDriver(d), 5 seconds).withDispatcher("akka.actor.views-dispatcher")

      case "morphline" => Props(
        classOf[DriverActor[MorphlineTransformation]],
        actionsRouter, ds, (d: DriverSettings) => MorphlineDriver(d), 5 seconds).withDispatcher("akka.actor.views-dispatcher")

      case _ => throw DriverException(s"Driver for ${driverName} not found")
    }
  }
}
