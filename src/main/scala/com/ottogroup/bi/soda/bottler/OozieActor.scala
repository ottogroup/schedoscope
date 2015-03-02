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
import org.joda.time.Chronology

class OozieActor(ds: DriverSettings) extends Actor {

  import context._
  val log = Logging(system, this)
  val oozieDriver =  OozieDriver(config)
  var startTime = new LocalDateTime()
  val oozieDriver = OozieDriver(ds)

  def running(jobId: String, s: ActorRef): Receive = LoggingReceive {
    case "tick" =>

      {
        try {
          val jobInfo = oozieDriver.getJobInfo(jobId)
          jobInfo.getStatus() match {
            case WorkflowJob.Status.RUNNING | WorkflowJob.Status.PREP | WorkflowJob.Status.SUSPENDED => {
              system.scheduler.scheduleOnce(5 seconds, self, "tick")
            }
            case WorkflowJob.Status.SUCCEEDED => {
              log.info(s"workflow ${jobId} succeeded")
              s ! OozieSuccess()
              startTime = new LocalDateTime()
              become(receive)
            }
            case WorkflowJob.Status.FAILED | WorkflowJob.Status.KILLED => {
              log.warning(s"workflow ${jobId} failed!")
              s ! OozieError()
              startTime = new LocalDateTime()
              become(receive)
            }
          }
        } catch {
          case e: Throwable => {
            log.error("unknown oozie error", e)
            s ! OozieError()
            become(receive)
          }
        }

      }
    case KillAction => {
      oozieDriver.kill(jobId)
      become(receive)
    }
    case _: GetStatus => sender() ! new OozieStatusResponse("executing job ", self, ProcessStatus.RUNNING, jobId, startTime)

  }

  def receive = LoggingReceive {
    case _: GetStatus => sender ! OozieStatusResponse("idle", self, ProcessStatus.IDLE, "", startTime)
    case CommandWithSender(d: Deploy, s) => oozieDriver.deployAll(ds)
    case WorkAvailable => sender ! PollCommand("oozie")
    case CommandWithSender(OozieTransformation(bundle, wf, appPath, conf), s) => {
      try {
        val jobId = oozieDriver.run(OozieTransformation(bundle, wf, appPath, conf))
        startTime = new LocalDateTime()

        val jobstatus = oozieDriver.getJobInfo(jobId).getStatus()
        if (jobstatus == WorkflowJob.Status.RUNNING ||
          jobstatus == WorkflowJob.Status.PREP) {
          become(running(jobId, s))
          system.scheduler.scheduleOnce(1000 millis, self, "tick")
        }
      } catch {
        case e: OozieClientException =>
          {
            log.warning("got exception..." + e.getMessage())
            s ! OozieException(e)
          }
        case e: NullPointerException => {
          log.warning("got exception..." + e.getMessage())
          s ! OozieException(e)
        }
        case e: Throwable => {
          log.warning("got exception..." + e.getMessage())
          s ! OozieException(e)

        }
      }
    }
  }
}

object OozieActor {
  def props(ds: DriverSettings) = Props(new OozieActor(ds))
}