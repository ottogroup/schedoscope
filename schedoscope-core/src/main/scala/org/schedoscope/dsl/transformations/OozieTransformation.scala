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

import java.io.FileInputStream
import java.io.InputStream
import java.util.Properties
import org.apache.commons.lang.StringUtils
import org.schedoscope.Settings
import org.schedoscope.dsl.Transformation
import scala.collection.JavaConversions._

case class OozieTransformation(bundle: String, workflow: String, workflowAppPath: String) extends Transformation {
  override def name = "oozie"

  override def resources() = {
    List(workflowAppPath)
  }

  description = StringUtils.abbreviate(s"${bundle}/${workflow}", 100)
}

object OozieTransformation {
  def oozieWFPath(bundle: String, workflow: String) = s"${Settings().getDriverSettings("oozie").location}/workflows/${bundle}/${workflow}/"

  def configurationFrom(inputStream: InputStream): Map[String, String] = {
    val props = new Properties()

    try {
      props.load(inputStream)
    } catch {
      case t: Throwable =>
    }

    Map() ++ props
  }

  def configurationFrom(filePath: String): Map[String, String] = try
    configurationFrom(new FileInputStream(filePath))
  catch {
    case t: Throwable => Map()
  }

  def configurationFromResource(resourcePath: String): Map[String, String] =
    try
      configurationFrom(getClass().getClassLoader().getResourceAsStream(resourcePath))
    catch {
      case t: Throwable => Map()
    }
}