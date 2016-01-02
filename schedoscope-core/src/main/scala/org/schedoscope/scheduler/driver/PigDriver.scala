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
import java.util.Properties

import org.apache.hadoop.security.UserGroupInformation
import org.apache.pig.{ ExecType, PigException, PigServer }
import org.joda.time.LocalDateTime
import org.schedoscope.{ DriverSettings, Schedoscope }
import org.schedoscope.dsl.transformations.PigTransformation
import org.schedoscope.dsl.transformations.Transformation.replaceParameters
import org.slf4j.LoggerFactory

import scala.concurrent.Future

/**
 * Driver for Pig transformations.
 */
class PigDriver(val driverRunCompletionHandlerClassNames: List[String], val ugi: UserGroupInformation) extends Driver[PigTransformation] {

  /**
   * Set transformation name to pig
   */
  override def transformationName = "pig"

  val log = LoggerFactory.getLogger(classOf[PigDriver])

  val driver = this // needed to access driver within ugi.doAs below

  /**
   * Construct a future-based driver run handle
   */
  def run(t: PigTransformation): DriverRunHandle[PigTransformation] =
    new DriverRunHandle[PigTransformation](this, new LocalDateTime(), t, Future {
      // FIXME: future work: register jars, custom functions
      executePigTransformation(t.latin, t.dirsToDelete, t.defaultLibraries, t.configuration.toMap, t.getView())
    })

  def executePigTransformation(latin: String, directoriesToDelete: List[String], libraries: List[String], conf: Map[String, Any], view: String): DriverRunState[PigTransformation] = {
    val actualLatin = replaceParameters(latin, conf)

    val props = new Properties()
    conf.foreach(c => props.put(c._1, c._2.asInstanceOf[Object]))

    ugi.doAs(new PrivilegedAction[DriverRunState[PigTransformation]]() {
      def run(): DriverRunState[PigTransformation] = {
        val ps = new PigServer(ExecType.valueOf(conf.getOrElse("exec.type", "MAPREDUCE").toString), props)
        try {
          // FIXME: we're doing parameter replacement by ourselves, because registerQuery doesn't support to specify parameters like
          // registerScript does (and attention: pig doesn't support variable names containing a dot).
          // another workaround would be: ps.registerScript(IOUtils.toInputStream(latin, "UTF-8") , conf.filter(!_._1.contains(".")).map(c => (c._1, c._2.toString)))
          ps.setJobName(view)
          directoriesToDelete.foreach(d => ps.deleteFile(d))
          libraries.foreach(l => ps.registerJar(l))
          ps.registerQuery(actualLatin)
          DriverRunSucceeded[PigTransformation](driver, s"Pig script ${actualLatin} executed")
        } catch {
          // FIXME: do we need special handling for some exceptions here (similar to hive?)
          case e: PigException =>
            DriverRunFailed(driver, s"PigException encountered while executing pig script ${actualLatin}; Stacktrace is: ${e.getStackTrace}", e)
          case t: Throwable => throw DriverException(s"Runtime exception while executing pig script ${actualLatin}", t)
        }
      }
    })
  }
}

/**
 * Factory for Pig driver
 */
object PigDriver {
  def apply(ds: DriverSettings) =
    new PigDriver(ds.driverRunCompletionHandlers, Schedoscope.settings.userGroupInformation)

}
