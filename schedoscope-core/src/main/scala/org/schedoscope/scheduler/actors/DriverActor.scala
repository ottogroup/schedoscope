/**
 * Copyright 2015 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schedoscope.scheduler.actors

import scala.language.postfixOps
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import org.schedoscope.DriverSettings
import org.schedoscope.SchedoscopeSettings
import org.schedoscope.scheduler.driver.Driver
import org.schedoscope.scheduler.driver.DriverException
import org.schedoscope.scheduler.driver.DriverRunFailed
import org.schedoscope.scheduler.driver.DriverRunHandle
import org.schedoscope.scheduler.driver.DriverRunOngoing
import org.schedoscope.scheduler.driver.DriverRunState
import org.schedoscope.scheduler.driver.DriverRunSucceeded
import org.schedoscope.scheduler.driver.FileSystemDriver
import org.schedoscope.scheduler.driver.HiveDriver
import org.schedoscope.scheduler.driver.OozieDriver
import org.schedoscope.scheduler.driver.ShellDriver
import org.schedoscope.dsl.transformations._
import org.schedoscope.scheduler.messages._
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.event.Logging
import akka.event.LoggingReceive
import org.schedoscope.scheduler.driver.MorphlineDriver
import org.schedoscope.scheduler.driver.MapreduceDriver
import org.schedoscope.scheduler.driver.PigDriver
import org.schedoscope.scheduler.driver.DriverException

/**
 * A driver actor manages the executions of transformations using hive, oozie etc. The actual
 * execution is done using a driver trait implementation. The driver actor code itself is transformation
 * type agnostic. Driver actors poll the transformation tasks they execute from the transformation manager actor
 *
 */
class DriverActor[T <: Transformation](transformationManagerActor: ActorRef, ds: DriverSettings, driverConstructor: (DriverSettings) => Driver[T], pingDuration: FiniteDuration) extends Actor {
  import context._
  val log = Logging(system, this)

  lazy val driver = driverConstructor(ds)

  var runningCommand: Option[CommandWithSender] = None

  /**
   * Start ticking upon start.
   */
  override def preStart() {
    logStateInfo("idle", "DRIVER ACTOR: initialized actor")
    tick()
  }

  /**
   * If the driver actor is being restarted by the transformation manager actor, the currently running action is reenqueued so it does not get lost.
   */
  override def preRestart(reason: Throwable, message: Option[Any]) {
    if (runningCommand.isDefined)
      transformationManagerActor ! runningCommand.get
  }

  /**
   * Provide continuous ticking in default state
   */
  def tick() {
    system.scheduler.scheduleOnce(pingDuration, self, "tick")
  }

  /**
   * Message handler for the default state.
   * Transitions only to state running, keeps polling the action manager for new work
   */
  def receive = LoggingReceive {
    case CommandWithSender(command, sender) => toRunning(CommandWithSender(command, sender))

    case "tick" => {
      transformationManagerActor ! PollCommand(driver.transformationName)
      tick()
    }
  }

  /**
   * Message handler for the running state
   * @param runHandle  reference to the running driver
   * @param originalSender reference to the viewActor that requested the transformation (for sending back the result)
   */
  def running(runHandle: DriverRunHandle[T], originalSender: ActorRef): Receive = LoggingReceive {
    case KillCommand() => {
      driver.killRun(runHandle)
      toReceive()
    }
    // If getting a command while being busy, reschedule it by sending it to the actionsmanager
    // Should this ever happen?
    case c: CommandWithSender => transformationManagerActor ! c

    // check all 10 seconds the state of the current running driver
    case "tick" => try {
      driver.getDriverRunState(runHandle) match {
        case _: DriverRunOngoing[T] => tick()

        case success: DriverRunSucceeded[T] => {
          log.info(s"DRIVER ACTOR: Driver run for handle=${runHandle} succeeded.")

          try {
            driver.driverRunCompleted(runHandle)
          } catch {
            case d: DriverException => throw d

            case t: Throwable => {
              log.error(s"DRIVER ACTOR: Driver run for handle=${runHandle} failed because completion handler threw exception ${t}")
              originalSender ! TransformationFailure(runHandle, DriverRunFailed[T](driver, "Completition handler failed", t))
              toReceive()
              tick()
            }
          }

          originalSender ! TransformationSuccess(runHandle, success)
          toReceive()
          tick()
        }

        case failure: DriverRunFailed[T] => {
          log.error(s"DRIVER ACTOR: Driver run for handle=${runHandle} failed. ${failure.reason}, cause ${failure.cause}, trace ${if (failure.cause != null) failure.cause.getStackTrace else "no trace available"}")

          try {
            driver.driverRunCompleted(runHandle)
          } catch {
            case d: DriverException => throw d

            case t: Throwable => {
            }
          }

          originalSender ! TransformationFailure(runHandle, failure)
          toReceive()
          tick()
        }
      }
    } catch {
      case exception: DriverException => {
        log.error(s"DRIVER ACTOR: Driver exception caught by driver actor in running state, rethrowing: ${exception.message}, cause ${exception.cause}, trace ${exception.getStackTrace}")
        throw exception
      }

      case t: Throwable => {
        log.error(s"DRIVER ACTOR: Unexpected exception caught by driver actor in running state, rethrowing: ${t.getMessage()}, cause ${t.getCause()}, trace ${t.getStackTrace}")
        throw t
      }
    }
  }

  /**
   *  State transition to default state.
   */
  def toReceive() {
    runningCommand = None

    logStateInfo("idle", "DRIVER ACTOR: becoming idle")

    become(receive)
  }

  /**
   * State transition to running state.
   *
   * Includes special handling of "Deploy" commands, those are executed directly, no state transition despite name of function
   * Otherwise run the transformation using the driver instance and switch to running state
   *
   * @param commandToRun
   */
  def toRunning(commandToRun: CommandWithSender) {
    runningCommand = Some(commandToRun)

    try {
      if (commandToRun.command.isInstanceOf[DeployCommand]) {

        logStateInfo("deploy", s"DRIVER ACTOR: Running Deploy command")

        driver.deployAll(ds)
        commandToRun.sender ! DeployCommandSuccess()

        logStateInfo("idle", "DRIVER ACTOR: becoming idle")

        runningCommand = None
      } else {
        val runHandle = driver.run(commandToRun.command.asInstanceOf[T])

        logStateInfo("running", s"DRIVER ACTOR: Running command ${commandToRun}, runHandle=${runHandle}", runHandle, driver.getDriverRunState(runHandle))

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
    transformationManagerActor ! TransformationStatusResponse(state, self, driver, runHandle, runState)
    log.info(message)
  }
}

/**
 * Factory methods for driver actors.
 */
object DriverActor {
  def props(settings: SchedoscopeSettings, driverName: String, transformationManager: ActorRef) = {
    val ds = settings.getDriverSettings(driverName)

    driverName match {
      case "hive" => Props(
        classOf[DriverActor[HiveTransformation]],
        transformationManager, ds, (ds: DriverSettings) => HiveDriver(ds), 5 seconds).withDispatcher("akka.actor.driver-dispatcher")

      case "mapreduce" => Props(
        classOf[DriverActor[MapreduceTransformation]],
        transformationManager, ds, (ds: DriverSettings) => MapreduceDriver(ds), 5 seconds).withDispatcher("akka.actor.driver-dispatcher")

      case "pig" => Props(
        classOf[DriverActor[PigTransformation]],
        transformationManager, ds, (ds: DriverSettings) => PigDriver(ds), 5 seconds).withDispatcher("akka.actor.driver-dispatcher")

      case "filesystem" => Props(
        classOf[DriverActor[FilesystemTransformation]],
        transformationManager, ds, (ds: DriverSettings) => FileSystemDriver(ds), 100 milliseconds).withDispatcher("akka.actor.driver-dispatcher")

      case "oozie" => Props(
        classOf[DriverActor[OozieTransformation]],
        transformationManager, ds, (ds: DriverSettings) => OozieDriver(ds), 5 seconds).withDispatcher("akka.actor.driver-dispatcher")

      case "morphline" => Props(
        classOf[DriverActor[MorphlineTransformation]],
        transformationManager, ds, (ds: DriverSettings) => MorphlineDriver(ds), 5 seconds).withDispatcher("akka.actor.driver-dispatcher")

      case "shell" => Props(
        classOf[DriverActor[ShellTransformation]],
        transformationManager, ds, (ds: DriverSettings) => ShellDriver(ds), 5 seconds).withDispatcher("akka.actor.driver-dispatcher")

      case _ => throw DriverException(s"Driver for ${driverName} not found")
    }
  }
}
