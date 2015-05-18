package org.schedoscope.dsl.transformations

import scala.collection.JavaConversions._
import org.apache.hadoop.mapreduce.Job
import org.schedoscope.dsl.Transformation
import org.schedoscope.dsl.Version
import org.schedoscope.Settings
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.mapreduce.MRJobConfig
import java.net.URI

case class MapreduceTransformation(createJob: (Map[String, Any]) => Job, directoriesToDelete: List[String], c: Map[String, Any]) extends Transformation {

  configureWith(c)

  override def name = "mapreduce"

  lazy val job = createJob(configuration.toMap)

  // resource hash based on MR job jar (in HDFS)
  override def versionDigest = Version.digest(Version.resourceHashes(resources()))

  description = StringUtils.abbreviate(createJob(configuration.toMap).getJobName, 100)

  override def resources() = {
    val jarName = try {
      job.getConfiguration().get(MRJobConfig.JAR).split("/").last
    } catch {
      case _: Throwable => null
    }

    Settings().getDriverSettings("mapreduce").libJarsHdfs
      .filter(lj => jarName == null || lj.contains(jarName))
  }

  def configure() {
    // if job jar hasn't been registered, add all mapreduce libjars
    // to distributed cache
    if (job.getConfiguration().get(MRJobConfig.JAR) == null) {
      resources().foreach(r => {
        try {
          job.addCacheFile(new URI(r))
        } catch {
          case _: Throwable => Unit
        }
      })
    }
    configuration.foreach(c => job.getConfiguration.set(c._1, c._2.toString))
  }
}
