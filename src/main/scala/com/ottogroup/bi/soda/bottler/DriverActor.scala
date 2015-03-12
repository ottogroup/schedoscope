package com.ottogroup.bi.soda.bottler

import akka.actor.Actor
import java.util.Properties
import org.apache.oozie.client.OozieClient
import java.io.FileReader
import org.apache.hadoop.security.UserGroupInformation
import org.apache.oozie.client.OozieClientException
import akka.actor.Props
import org.apache.oozie.client.WorkflowJob
import scala.concurrent.duration._
import akka.actor.ActorRef
import akka.event.Logging
import akka.event.LoggingReceive
import com.ottogroup.bi.soda.dsl.transformations.oozie.OozieTransformation
import com.ottogroup.bi.soda.bottler.driver.OozieDriver._
import org.joda.time.LocalDateTime
import com.ottogroup.bi.soda.bottler.driver.OozieDriver
import com.typesafe.config.Config
import com.ottogroup.bi.soda.bottler.api.DriverSettings
import com.ottogroup.bi.soda.bottler.driver.DriverRunHandle
import com.ottogroup.bi.soda.bottler.driver.DriverRunOngoing
import com.ottogroup.bi.soda.bottler.driver.DriverRunSucceeded
import com.ottogroup.bi.soda.bottler.driver.DriverRunFailed
import com.ottogroup.bi.soda.bottler.driver.DriverException
import com.ottogroup.bi.soda.bottler.driver.Driver
import com.ottogroup.bi.soda.dsl.Transformation
import com.ottogroup.bi.soda.bottler.api.Settings
import com.ottogroup.bi.soda.bottler.driver.HiveDriver
import com.ottogroup.bi.soda.bottler.driver.FileSystemDriver
import akka.routing.BroadcastRouter

class DriverActor[T <: Transformation](val actionsRouter: ActorRef, val driver: Driver[T], val ds: DriverSettings, val pingDuration: FiniteDuration) extends Actor {
  import context._
  val log = Logging(system, this)
  var startTime = new LocalDateTime

  override def preStart() {
    system.scheduler.scheduleOnce(pingDuration, self, "tick")
  }

  def running(runHandle: DriverRunHandle[T], s: ActorRef): Receive = LoggingReceive {
    case "tick" => try {
      driver.getDriverRunState(runHandle) match {
        case _: DriverRunOngoing[T] => system.scheduler.scheduleOnce(pingDuration, self, "tick")

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
        log.error(s"Driver exception caught: ${exception.message}, cause ${exception.cause}")
        s ! ActionExceptionFailure(runHandle, exception)
        becomeReceive()
      }
    }

    case KillAction => {
      driver.killRun(runHandle)
      becomeReceive()
    }

    case _: GetStatus => sender() ! ActionStatusResponse("running", self, driver, runHandle, driver.getDriverRunState(runHandle))
  }

  def becomeReceive() {
    startTime = new LocalDateTime()
    unbecome()
    become(receive)
    system.scheduler.scheduleOnce(pingDuration, self, "tick")
  }

  def receive = LoggingReceive {
    case _: GetStatus => sender ! ActionStatusResponse("idle", self, driver, null, null)

    case CommandWithSender(d: Deploy, s) => driver.deployAll(ds)

    case "tick" => {
      actionsRouter ! PollCommand(driver.name)
      system.scheduler.scheduleOnce(pingDuration, self, "tick")
    }

    case CommandWithSender(t, s) => {
      val runHandle = driver.run(t.asInstanceOf[T])
      unbecome()
      become(running(runHandle, s))
      system.scheduler.scheduleOnce(pingDuration, self, "tick")
    }
  }
}

object DriverActor {
  def props(driverName: String, actionsRouter: ActorRef) = {
    val ds = Settings().getDriverSettings(driverName)

    val driverActor = driverName match {
      case "hive" => Props(new DriverActor(actionsRouter, HiveDriver(ds), ds, 1 seconds))
      case "filesystem" => Props(new DriverActor(actionsRouter, FileSystemDriver(ds), ds, 100 milliseconds))
      case "oozie" => Props(new DriverActor(actionsRouter, OozieDriver(ds), ds, 5 seconds))

      case _ => throw DriverException(s"Driver for ${driverName} not found")
    }

    driverActor.withRouter(BroadcastRouter(nrOfInstances = ds.concurrency))
  }
}