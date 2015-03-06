package com.ottogroup.bi.soda.bottler.driver

import java.util.Properties
import com.typesafe.config.ConfigFactory
import org.apache.oozie.client.OozieClient
import scala.collection.JavaConversions._
import org.apache.hadoop.security.UserGroupInformation
import java.io.FileOutputStream
import com.ottogroup.bi.soda.bottler.OozieCommand
import java.io.File
import com.ottogroup.bi.soda.dsl.transformations.oozie.OozieTransformation
import com.ottogroup.bi.soda.bottler.OozieCommand
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

  def run(t: OozieTransformation): DriverRunHandle[OozieTransformation] = {
    val jobConf = createOozieJobConf(t)
    val oozieJobId = runOozieJob(jobConf)
    new DriverRunHandle[OozieTransformation](this, new LocalDateTime(), t, oozieJobId, null)
  }

  override def runAndWait(t: OozieTransformation): DriverRunState[OozieTransformation] = {
    val jobConf = createOozieJobConf(t)
    println("Starting Oozie job with config: \n" + jobConf.mkString("\n"))
    runAndWait(jobConf)
  }

  def runOozieJob(jobProperties: Properties): String = {
    println("Starting Oozie job with config: \n" + jobProperties.mkString("\n"))
    client.run(jobProperties)
  }

  def runAndWait(jobProperties: Properties): Boolean = {
    import WorkflowJob.Status._

    val jobId = runOozieJob(jobProperties)

    while (client.getJobInfo(jobId).getStatus() == RUNNING ||
      client.getJobInfo(jobId).getStatus() == PREP) {
      println("Job status is " + client.getJobInfo(jobId).getStatus())
      Thread.sleep(1000)
    }

    println("Job status is " + client.getJobInfo(jobId).getStatus())

    client.getJobInfo(jobId).getStatus() match {
      case SUCCEEDED => true
      case _ => false
    }
  }

  def getJobInfo(jobId: String) = client.getJobInfo(jobId)

  def kill(jobId: String) = client.kill(jobId)

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