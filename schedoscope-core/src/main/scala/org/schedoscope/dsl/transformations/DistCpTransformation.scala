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
package org.schedoscope.dsl.transformations

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.tools.{DistCp, DistCpOptions}
import org.schedoscope.dsl.View
import org.schedoscope.scheduler.driver.{DriverRunState, MapreduceDriver}

import scala.collection.JavaConverters._

object DistCpTransformation {

  def copyToView(sourceView: View, targetView: View): DistCpTransformation = {
    val target = targetView.fullPath.split("/").dropRight(1).mkString("/")
    DistCpTransformation(targetView, List(sourceView.fullPath), target)
  }

  def copyToDirToView(sourcePath: String, targetView: View): DistCpTransformation = {
    val target = targetView.fullPath.split("/").drop(1).mkString("/")
    DistCpTransformation(targetView, List(sourcePath), target)
  }

  def copyToFileToView(sourceFile: String, targetView: View): DistCpTransformation = {
    DistCpTransformation(targetView, List(sourceFile), targetView.fullPath)
  }

}

case class DistCpTransformation(v: View,
                                var sources: List[String],
                                var target: String,
                                deleteViewPath: Boolean = false,
                                config: Configuration = new Configuration())
  extends MapreduceBaseTransformation {

  var directoriesToDelete = if (deleteViewPath) List(v.fullPath) else List()

  override def stringsToChecksum: List[String] = target :: sources


  override val cleanupAfterJob: (Job, MapreduceDriver, DriverRunState[MapreduceBaseTransformation]) =>
    DriverRunState[MapreduceBaseTransformation] = (_, __, completionRunState) => completionRunState

  lazy val job: Job = {
    val distCp = new DistCp(config, distCpOptions)
    val createJob = distCp.getClass.getDeclaredMethod("createJob")
    createJob.setAccessible(true)
    val job = createJob.invoke(distCp).asInstanceOf[Job]
    val prepareFileListing = distCp.getClass.getDeclaredMethod("prepareFileListing", job.getClass)
    prepareFileListing.setAccessible(true)
    prepareFileListing.invoke(distCp, job)
    job
  }

  def distCpOptions: DistCpOptions = if (configuration.nonEmpty) {
    DistCpConfiguration
      .fromConfig(configuration.toMap)
      .toDistCpOptions(sources.map(new Path(_)), new Path(target))
  } else {
    val s = sources.map(new Path(_)).asJava
    new DistCpOptions(s, new Path(target))
  }
}




