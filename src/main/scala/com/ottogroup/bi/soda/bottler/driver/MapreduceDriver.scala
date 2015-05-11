package com.ottogroup.bi.soda.bottler.driver

import java.util.Properties
import scala.collection.JavaConversions.asScalaSet
import scala.collection.JavaConversions.propertiesAsScalaMap
import scala.concurrent.duration.Duration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.oozie.client.OozieClient
import org.joda.time.LocalDateTime
import com.ottogroup.bi.soda.DriverSettings
import com.ottogroup.bi.soda.Settings
import com.ottogroup.bi.soda.dsl.transformations.MapreduceTransformation
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.JobStatus.State.FAILED
import org.apache.hadoop.mapreduce.JobStatus.State.KILLED
import org.apache.hadoop.mapreduce.JobStatus.State.PREP
import org.apache.hadoop.mapreduce.JobStatus.State.RUNNING
import org.apache.hadoop.mapreduce.JobStatus.State.SUCCEEDED

class MapreduceDriver(val ugi: UserGroupInformation) extends Driver[MapreduceTransformation] {

  override def transformationName = "mapreduce"

  def run(t: MapreduceTransformation): DriverRunHandle[MapreduceTransformation] = try {
    t.job.submit()
    new DriverRunHandle[MapreduceTransformation](this, new LocalDateTime(), t, t.job)
  } catch {
    case e: Throwable => throw DriverException("Unexpected error occurred while running Mapreduce job", e)
  }

  override def getDriverRunState(run: DriverRunHandle[MapreduceTransformation]) = {
    val job = run.stateHandle.asInstanceOf[Job]
    val jobId = job.getJobName
    try {
      job.getJobState match {
        case SUCCEEDED       => DriverRunSucceeded[MapreduceTransformation](this, s"Mapreduce job ${jobId} succeeded")
        case PREP | RUNNING  => DriverRunOngoing[MapreduceTransformation](this, run)
        case FAILED | KILLED => DriverRunFailed[MapreduceTransformation](this, s"Mapreduce job ${jobId} failed", DriverException(s"Failed Mapreduce job status ${job.getJobState}"))
      }
    } catch {
      case e: Throwable => throw DriverException(s"Unexpected error occurred while checking run state of Oozie job ${jobId}", e)
    }

  }
  
  override def runAndWait(t: MapreduceTransformation): DriverRunState[MapreduceTransformation] = {
    val started = new LocalDateTime()
    t.job.waitForCompletion(true);
    val ret = getDriverRunState(new DriverRunHandle[MapreduceTransformation](this, started, t, t.job))
    println(ret)
    ret
  }

  
  override def killRun(run: DriverRunHandle[MapreduceTransformation]) = {
    val job = run.stateHandle.asInstanceOf[Job]
    try {
      job.killJob()
    } catch {
      case e: Throwable => throw DriverException(s"Unexpected error occurred while killing Oozie job ${run.stateHandle}", e)
    }
  }
}


object MapreduceDriver {
  def apply(ds: DriverSettings) = new MapreduceDriver(Settings().userGroupInformation)
}