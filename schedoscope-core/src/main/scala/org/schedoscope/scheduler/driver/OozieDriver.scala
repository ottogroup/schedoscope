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
package org.schedoscope.scheduler.driver

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.security.UserGroupInformation
import org.apache.oozie.client.OozieClient
import org.apache.oozie.client.WorkflowJob.Status.{ PREP, RUNNING, SUCCEEDED, SUSPENDED }
import org.joda.time.LocalDateTime
import org.schedoscope.conf.DriverSettings
import org.schedoscope.dsl.transformations.OozieTransformation

import scala.collection.JavaConversions.asScalaSet

/**
 * This driver performs Oozie transformations.
 */
class OozieDriver(val driverRunCompletionHandlerClassNames: List[String], val client: OozieClient) extends DriverOnNonBlockingApi[OozieTransformation] {

  def transformationName = "oozie"

  def run(t: OozieTransformation): DriverRunHandle[OozieTransformation] = try {
    val jobConf = createOozieJobConf(t)
    val oozieJobId = runOozieJob(jobConf)
    new DriverRunHandle[OozieTransformation](this, new LocalDateTime(), t, oozieJobId)
  } catch {
    case e: Throwable => throw RetryableDriverException("Unexpected error occurred while running Oozie job", e)
  }

  def getDriverRunState(run: DriverRunHandle[OozieTransformation]) = {
    val jobId = run.stateHandle.toString
    try {
      val state = getJobInfo(jobId).getStatus()

      state match {
        case SUCCEEDED                  => DriverRunSucceeded[OozieTransformation](this, s"Oozie job ${jobId} succeeded")
        case SUSPENDED | RUNNING | PREP => DriverRunOngoing[OozieTransformation](this, run)
        case _                          => DriverRunFailed[OozieTransformation](this, s"Oozie job ${jobId} failed", RetryableDriverException(s"Failed Oozie job status ${state}"))
      }
    } catch {
      case e: Throwable => throw RetryableDriverException(s"Unexpected error occurred while checking run state of Oozie job ${jobId}", e)
    }
  }

  override def killRun(run: DriverRunHandle[OozieTransformation]) = {
    val jobId = run.stateHandle.toString
    try {
      client.kill(jobId)
    } catch {
      case e: Throwable => throw RetryableDriverException(s"Unexpected error occurred while killing Oozie job ${run.stateHandle}", e)
    }
  }

  def runOozieJob(jobProperties: Properties): String = client.run(jobProperties)

  def getJobInfo(jobId: String) = client.getJobInfo(jobId)

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

/**
 * Factory for Oozie drivers
 */
object OozieDriver {
  def apply(ds: DriverSettings) = new OozieDriver(ds.driverRunCompletionHandlers, new OozieClient(ds.url))
}