package com.ottogroup.bi.soda.bottler

import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import org.joda.time.LocalDateTime
import com.ottogroup.bi.soda.bottler.api.DriverSettings
import com.ottogroup.bi.soda.bottler.api.Settings
import com.ottogroup.bi.soda.bottler.driver.Driver
import com.ottogroup.bi.soda.bottler.driver.DriverException
import com.ottogroup.bi.soda.bottler.driver.DriverRunFailed
import com.ottogroup.bi.soda.bottler.driver.DriverRunHandle
import com.ottogroup.bi.soda.bottler.driver.DriverRunOngoing
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
import akka.routing.BroadcastRouter
import com.ottogroup.bi.soda.dsl.transformations.sql.HiveTransformation
import com.ottogroup.bi.soda.dsl.transformations.filesystem.FilesystemTransformation
import com.ottogroup.bi.soda.dsl.transformations.oozie.OozieTransformation

class DriverActor[T <: Transformation](val actionsRouter: ActorRef, val ds: DriverSettings, val driverConstructor: (DriverSettings) => Driver[T], val pingDuration: FiniteDuration) extends Actor {
  import context._
  val log = Logging(system, this)
  var startTime = new LocalDateTime

  lazy val driver = driverConstructor(ds)

  def receive = LoggingReceive {
    case _: GetStatus => sender ! ActionStatusResponse("idle", self, driver, null, null)

    case CommandWithSender(d: Deploy, s) => driver.deployAll(ds)

    case "tick" => {
      actionsRouter ! PollCommand(driver.name)
      tick()
    }

    case CommandWithSender(command, sender) => becomeRunning(sender, command.asInstanceOf[T])
  }

  def running(runHandle: DriverRunHandle[T], s: ActorRef): Receive = LoggingReceive {
    case "tick" => try {
      driver.getDriverRunState(runHandle) match {
        case _: DriverRunOngoing[T] => tick()

        case success: DriverRunSucceeded[T] => {
          log.info(s"Driver run ${runHandle} succeeded.")
          s ! ActionSuccess(runHandle, success)
          becomeReceive()
        }

        case failure: DriverRunFailed[T] => {
          log.info(s"Oozie workflow ${runHandle} failed. ${failure.reason}, cause ${failure.cause}")
          s ! ActionFailure(runHandle, failure)
          becomeReceive()
        }
      }
    } catch {
      case exception: DriverException => {
        log.error(s"Driver exception caught by driver actor in running state, rethrowing: ${exception.message}, cause ${exception.cause}")
        throw exception
      }
      case t: Throwable => {
        log.error(s"Unexpected exception caught by driver actor in running state, rethrowing: ${t.getMessage()}, cause ${t.getCause()}")
        throw t
      }
    }

    case KillAction => {
      driver.killRun(runHandle)
      becomeReceive()
    }

    case _: GetStatus => sender() ! ActionStatusResponse("running", self, driver, runHandle, driver.getDriverRunState(runHandle))
  }

  override def preStart() {
    tick()
  }

  def becomeReceive() {
    startTime = new LocalDateTime()
    unbecome()
    become(receive)
    tick()
  }

  def becomeRunning(sender: ActorRef, command: T) {
    try {
      val runHandle = driver.run(command)
      unbecome()
      become(running(runHandle, sender))
      tick()
    } catch {
      case exception: DriverException => {
        log.error(s"Driver exception caught by driver actor in receive state, rethrowing: ${exception.message}, cause ${exception.cause}")
        throw exception
      }
      case t: Throwable => {
        log.error(s"Unexpected exception caught by driver actor in receive state, rethrowing: ${t.getMessage()}, cause ${t.getCause()}")
        throw t
      }
    }
  }

  def tick() {
    system.scheduler.scheduleOnce(pingDuration, self, "tick")
  }
}

object DriverActor {
  def props(driverName: String, actionsRouter: ActorRef) = {
    val ds = Settings().getDriverSettings(driverName)

    driverName match {
      case "hive" => Props(
        classOf[DriverActor[HiveTransformation]],
        actionsRouter, ds, (_: DriverSettings) => HiveDriver(_), 5 seconds)

      case "filesystem" => Props(
        classOf[DriverActor[FilesystemTransformation]],
        actionsRouter, ds, (_: DriverSettings) => FileSystemDriver(_), 100 milliseconds)

      case "oozie" => Props(
        classOf[DriverActor[OozieTransformation]],
        actionsRouter, ds, (_: DriverSettings) => OozieDriver(_), 5 seconds)

      case _ => throw DriverException(s"Driver for ${driverName} not found")
    }
  }
}