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

class OozieActor(ds: DriverSettings) extends Actor {
  import context._
  val log = Logging(system, this)
  val oozieDriver = OozieDriver(ds)
  var startTime = new LocalDateTime

  def running(runHandle: DriverRunHandle[OozieTransformation], s: ActorRef): Receive = LoggingReceive {
    case "tick" => {
      try {
        val runState = oozieDriver.getDriverRunState(runHandle)

        runState match {
          case _: DriverRunOngoing[OozieTransformation] => system.scheduler.scheduleOnce(5 seconds, self, "tick")
          case _: DriverRunSucceeded[OozieTransformation] => {
            log.info(s"Oozie workflow ${runHandle.stateHandle} succeeded.")
            s ! OozieSuccess()
            startTime = new LocalDateTime()
            become(receive)
          }
          case f: DriverRunFailed[OozieTransformation] => {
            log.info(s"Oozie workflow ${runHandle.stateHandle} failed. ${f.reason}, cause ${f.cause}")
            s ! OozieError()
            startTime = new LocalDateTime()
            become(receive)
          }
        }
      } catch {
        case e: DriverException => {
          log.error(s"Ooozie Driver exception caught. ${e.message}, cause ${e.cause}")
          s ! OozieError()
          become(receive)
        }
      }

    }
    case KillAction => {
      oozieDriver.killRun(runHandle)
      become(receive)
    }
    case _: GetStatus => sender() ! new OozieStatusResponse("executing job ", self, ProcessStatus.RUNNING, runHandle.stateHandle.toString, startTime)
  }

  def receive = LoggingReceive {
    case _: GetStatus => sender ! OozieStatusResponse("idle", self, ProcessStatus.IDLE, "", startTime)
    case CommandWithSender(d: Deploy, s) => oozieDriver.deployAll(ds)
    case WorkAvailable => sender ! PollCommand("oozie")
    case CommandWithSender(OozieTransformation(bundle, wf, appPath, conf), s) => {
      val runHandle = oozieDriver.run(OozieTransformation(bundle, wf, appPath, conf))
      become(running(runHandle, s))
      system.scheduler.scheduleOnce(1000 millis, self, "tick")
    }
  }
}

object OozieActor {
  def props(ds: DriverSettings) = Props(new OozieActor(ds))
}