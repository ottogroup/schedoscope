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

import java.security.PrivilegedAction

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.JobStatus.State.{ FAILED, KILLED, PREP, RUNNING, SUCCEEDED }
import org.apache.hadoop.security.UserGroupInformation
import org.joda.time.LocalDateTime
import org.schedoscope.{ DriverSettings, Schedoscope }
import org.schedoscope.dsl.transformations.MapreduceTransformation

/**
 * Driver that executes Mapreduce transformations.
 */
class MapreduceDriver(val driverRunCompletionHandlerClassNames: List[String], val ugi: UserGroupInformation, val fileSystemDriver: FileSystemDriver) extends Driver[MapreduceTransformation] {

  override def transformationName = "mapreduce"

  /**
   * Start mapreduce job asynchronously and embed the job object as the run handle.
   */
  def run(t: MapreduceTransformation): DriverRunHandle[MapreduceTransformation] = try {

    ugi.doAs {

      new PrivilegedAction[DriverRunHandle[MapreduceTransformation]]() {

        def run(): DriverRunHandle[MapreduceTransformation] = {
          t.configure()
          t.directoriesToDelete.foreach(d => fileSystemDriver.delete(d, true))

          t.job.submit()

          new DriverRunHandle[MapreduceTransformation](driver, new LocalDateTime(), t, t.job)
        }
      }
    }
  } catch {
    case e: Throwable => throw RetryableDriverException("Unexpected error occurred while submitting Mapreduce job", e)
  }

  /**
   * Return run state for the mapreduce job given by the run handle
   */
  override def getDriverRunState(runHandle: DriverRunHandle[MapreduceTransformation]): DriverRunState[MapreduceTransformation] = try {

    val job = runHandle.stateHandle.asInstanceOf[Job]
    val jobId = job.getJobName
    val cleanupAfterJob = runHandle.transformation.cleanupAfterJob

    ugi.doAs {

      new PrivilegedAction[DriverRunState[MapreduceTransformation]]() {

        def run(): DriverRunState[MapreduceTransformation] = job.getJobState match {
          case PREP | RUNNING  => DriverRunOngoing[MapreduceTransformation](driver, runHandle)
          case FAILED | KILLED => cleanupAfterJob(job, DriverRunFailed[MapreduceTransformation](driver, s"Mapreduce job ${jobId} failed with state ${job.getJobState}", null))
          case SUCCEEDED       => cleanupAfterJob(job, DriverRunSucceeded[MapreduceTransformation](driver, s"Mapreduce job ${jobId} succeeded"))
        }

      }
    }
  } catch {
    case e: Throwable => throw RetryableDriverException(s"Unexpected error occurred while checking run state of Mapreduce job", e)
  }

  /**
   * Run mapreduce job, block, and return the result as a run state when job terminated.
   */
  override def runAndWait(t: MapreduceTransformation): DriverRunState[MapreduceTransformation] = try {

    ugi.doAs {

      new PrivilegedAction[DriverRunState[MapreduceTransformation]]() {

        def run(): DriverRunState[MapreduceTransformation] = {
          t.configure()
          t.directoriesToDelete.foreach(d => fileSystemDriver.delete(d, true))

          t.job.waitForCompletion(true)

          getDriverRunState(new DriverRunHandle[MapreduceTransformation](driver, new LocalDateTime(), t, t.job))
        }
      }
    }
  } catch {
    case e: Throwable => DriverRunFailed[MapreduceTransformation](driver, s"Mapreduce job ${t.job.getJobName} failed", e)
  }

  /**
   * Kill the mapreduce job behind the given run handle.
   */
  override def killRun(runHandle: DriverRunHandle[MapreduceTransformation]) = try {
    val job = runHandle.stateHandle.asInstanceOf[Job]

    ugi.doAs {

      new PrivilegedAction[Unit]() {

        def run() {
          job.killJob()
        }

      }
    }
  } catch {
    case e: Throwable => throw RetryableDriverException(s"Unexpected error occurred while killing Mapreduce job", e)
  }

  private def driver = this

  
}

/**
 * Factory for the mapreduce driver.
 */
object MapreduceDriver {
  def apply(ds: DriverSettings) = new MapreduceDriver(ds.driverRunCompletionHandlers, Schedoscope.settings.userGroupInformation, FileSystemDriver(Schedoscope.settings.getDriverSettings("filesystem")))
}
