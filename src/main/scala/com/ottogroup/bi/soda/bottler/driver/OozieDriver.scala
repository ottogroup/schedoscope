package com.ottogroup.bi.soda.bottler.driver

import java.util.Properties
import com.typesafe.config.ConfigFactory
import org.apache.oozie.client.OozieClient
import org.apache.oozie.client.WorkflowJob.Status._
import scala.collection.JavaConversions._
import org.apache.hadoop.security.UserGroupInformation
import java.io.FileOutputStream
import java.io.File
import com.ottogroup.bi.soda.dsl.transformations.oozie.OozieTransformation
import org.apache.oozie.client.WorkflowJob
import com.ottogroup.bi.soda.dsl.Transformation
import com.ottogroup.bi.soda.bottler.driver.OozieDriver._
import com.typesafe.config.Config
import com.ottogroup.bi.soda.bottler.api.DriverSettings
import com.ottogroup.bi.soda.bottler.api.Settings
import java.io.FileReader
import scala.concurrent.duration.Duration
import org.joda.time.LocalDateTime

class OozieDriver(val client: OozieClient) extends Driver[OozieTransformation] {

  override def runTimeOut: Duration = Settings().oozieActionTimeout

  override def name = "oozie"

  def run(t: OozieTransformation): DriverRunHandle[OozieTransformation] = try {
    val jobConf = createOozieJobConf(t)
    val oozieJobId = runOozieJob(jobConf)
    new DriverRunHandle[OozieTransformation](this, new LocalDateTime(), t, oozieJobId, null)
  } catch {
    case e: Throwable => throw DriverException("Unexpected error occurred while running Oozie job", e)
  }

  override def getDriverRunState(run: DriverRunHandle[OozieTransformation]) = {
    val jobId = run.stateHandle.toString
    try {
      val state = getJobInfo(jobId).getStatus()

      state match {
        case SUCCEEDED => DriverRunSucceeded[OozieTransformation](this, s"Oozie job ${jobId} succeeded")
        case SUSPENDED | RUNNING | PREP => DriverRunOngoing[OozieTransformation](this, run)
        case _ => DriverRunFailed[OozieTransformation](this, s"Oozie job ${jobId} failed", DriverException(s"Failed Oozie job status ${state}"))
      }
    } catch {
      case e: Throwable => throw DriverException(s"Unexpected error occurred while checking run state of Oozie job ${jobId}", e)
    }
  }

  override def runAndWait(t: OozieTransformation): DriverRunState[OozieTransformation] = {
    val runHandle = run(t)

    while (getDriverRunState(runHandle).isInstanceOf[DriverRunOngoing[OozieTransformation]])
      Thread.sleep(5000)

    getDriverRunState(runHandle)
  }

  override def killRun(run: DriverRunHandle[OozieTransformation]) = {
    val jobId = run.stateHandle.toString
    println("Killing Oozie job ${jobId}")
    try {
      client.kill(jobId)
    } catch {
      case e: Throwable => throw DriverException(s"Unexpected error occurred while killing Oozie job ${run.stateHandle}", e)
    }
  }

  def runOozieJob(jobProperties: Properties): String = {
    println("Starting Oozie job with config: \n" + jobProperties.mkString("\n"))
    client.run(jobProperties)
  }

  def getJobInfo(jobId: String) = {
    val jobInfo = client.getJobInfo(jobId)
    println(s"Oozie job ${jobId} info is ${jobInfo}")
    jobInfo
  }

  def createOozieJobConf(wf: OozieTransformation): Properties =
    wf match {
      case o: OozieTransformation => {
        val properties = new Properties()
        o.configuration.foreach(c => properties.put(c._1, c._2.toString()))

        properties.put(OozieClient.APP_PATH, wf.workflowAppPath)
        properties.remove(OozieClient.BUNDLE_APP_PATH)
        properties.remove(OozieClient.COORDINATOR_APP_PATH)

        // resolve embedded variables
        val config = ConfigFactory.parseProperties(properties).resolve()
        config.entrySet().foreach(e => properties.put(e.getKey(), e.getValue().unwrapped().toString()))
        if (!properties.containsKey("user.name"))
          properties.put("user.name", UserGroupInformation.getLoginUser().getUserName());
        properties
      }
    }
}

object OozieDriver {
  def apply(ds: DriverSettings) = new OozieDriver(new OozieClient(ds.url))
}