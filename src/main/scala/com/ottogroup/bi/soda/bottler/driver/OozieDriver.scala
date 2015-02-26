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

class OozieDriver(val client: OozieClient) extends Driver {

  override def run(t: Transformation): String = {
    t match {
      case ot: OozieTransformation => {
        val jobConf = createOozieJobConf(ot)
        println("Starting Oozie job with config: \n" + jobConf.mkString("\n"))
        runOozieJob(jobConf)
      }
      case _ => throw new RuntimeException("OozieDriver can only run OozieWF transformations.")
    }
  }

  override def runAndWait(t: Transformation): Boolean = {
    t match {
      case ot: OozieTransformation => {
        val jobConf = createOozieJobConf(ot)
        println("Starting Oozie job with config: \n" + jobConf.mkString("\n"))
        runAndWait(jobConf)
      }
      case _ => throw new RuntimeException("OozieDriver can only run OozieWF transformations.")
    }
  }

  def runOozieJob(jobProperties: Properties): String = {
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
}

object OozieDriver {
  def apply(ds: DriverSettings) = new OozieDriver(new OozieClient(ds.url))

  def createOozieJobConf(wf: OozieTransformation): Properties = {
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
}