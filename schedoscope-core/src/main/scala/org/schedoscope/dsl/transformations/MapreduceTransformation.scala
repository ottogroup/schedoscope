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

import java.net.URI

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.mapreduce.{ Job, MRJobConfig }
import org.schedoscope.Schedoscope
import org.schedoscope.dsl.View
import org.schedoscope.scheduler.driver.{ MapreduceDriver, DriverRunState }

/**
 * Compute a view using a plain Map-Reduce job.
 *
 * @param view reference to the view being computed
 *
 * @param createJob function to create the MapReduce job object.
 * 									For this purpose, it is passed the transformation configuration map.
 * 									It must return the prepared job object.
 * @param cleanupAfterJob function to perform cleanup tasks after job completion.
 * 									It is passed the job object as well as the driver run state after job completion.
 * 									It must return a final run state.
 * 									The default does nothing and returns the run state after job completion unmolested.
 * @param dirsToDelete List of directories to empty before execution. Includes the view's fullPath
 *
 */
case class MapreduceTransformation(
    v: View,
    createJob: (Map[String, Any]) => Job,
    cleanupAfterJob: (Job, MapreduceDriver, DriverRunState[MapreduceTransformation]) => DriverRunState[MapreduceTransformation] = (_, __, completionRunState) => completionRunState,
    dirsToDelete: List[String] = List()) extends Transformation {

  def name = "mapreduce"

  lazy val job = createJob(configuration.toMap)

  var directoriesToDelete = dirsToDelete ++ List(v.fullPath)

  description = StringUtils.abbreviate(v.urlPath, 100)

  override def fileResourcesToChecksum = {
    val jarName = try {
      job.getConfiguration().get(MRJobConfig.JAR).split("/").last
    } catch {
      case _: Throwable => null
    }

    Schedoscope.settings
      .getDriverSettings("mapreduce")
      .libJarsHdfs
      .filter(lj => jarName == null || lj.contains(jarName))
  }

  def configure() {
    // if job jar hasn't been registered, add all mapreduce libjars
    // to distributed cache
    if (job.getConfiguration().get(MRJobConfig.JAR) == null) {
      fileResourcesToChecksum.foreach(r => {
        try {
          job.addCacheFile(new URI(r))
        } catch {
          case _: Throwable => Unit
        }
      })
    }
    configuration.foreach { case (k, v) => if (v == null) job.getConfiguration.unset(k) else job.getConfiguration.set(k, v.toString) }
  }
}
