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
import com.ottogroup.bi.soda.dsl.transformations.oozie.OozieWF
import com.ottogroup.bi.soda.bottler.driver.OozieDriver._
import org.joda.time.LocalDateTime
import com.ottogroup.bi.soda.bottler.driver.OozieDriver
import com.typesafe.config.Config

class OozieActor(config:Config) extends Actor {

  import context._
  val log = Logging(system, this)
  val oozieDriver =  OozieDriver(config)
  var startTime = LocalDateTime.now()

  def running(jobId: String, s: ActorRef): Receive = LoggingReceive {
    case "tick" =>

      {
        val jobInfo = oozieDriver.getJobInfo(jobId)
        log.info(s"workflow ${jobInfo.getId()} in state: " + jobInfo.getStatus())

        jobInfo.getStatus() match {
          case WorkflowJob.Status.RUNNING | WorkflowJob.Status.PREP | WorkflowJob.Status.SUSPENDED => {
            system.scheduler.scheduleOnce(10 seconds, self, "tick")
          }
          case WorkflowJob.Status.SUCCEEDED => {

            s ! OozieSuccess()
            startTime = LocalDateTime.now
            become(receive)
          }
          case WorkflowJob.Status.FAILED | WorkflowJob.Status.KILLED => {
            s ! OozieError()
            startTime = LocalDateTime.now
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
    case Deploy => oozieDriver.deployAll

    case WorkAvailable => sender ! PollCommand("oozie")
    case CommandWithSender(OozieWF(bundle, wf, appPath, conf), s) => {
      val jobProperties = createOozieJobConf(OozieWF(bundle, wf, appPath, conf))
      try {

        val jobId = oozieDriver.runOozieJob(jobProperties)
        startTime = LocalDateTime.now()
        if (oozieDriver.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING ||
          oozieDriver.getJobInfo(jobId).getStatus() == WorkflowJob.Status.PREP) {
          become(running(jobId, s))
          system.scheduler.scheduleOnce(1000 millis, self, "tick")
        }
        //sender ! OozieSuccess()
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
  def props(config:Config) = Props(new OozieActor(config))
}