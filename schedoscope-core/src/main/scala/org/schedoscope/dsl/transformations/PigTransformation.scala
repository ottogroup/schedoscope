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
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hive.metastore.api.Function
import org.apache.hadoop.hive.metastore.api.ResourceType
import org.apache.hadoop.hive.metastore.api.ResourceUri
import org.schedoscope.dsl.Version
import org.schedoscope.Settings
import org.schedoscope.dsl.Transformation
import org.schedoscope.dsl.View
import scala.collection.JavaConversions._
import org.apache.hcatalog.pig.HCatLoader

case class PigTransformation(latin: String, directoriesToDelete: List[String] = List()) extends Transformation {

  override def name = "pig"

  override def versionDigest = Version.digest(latin)

  description = "[..]" + StringUtils.abbreviate(latin.replaceAll("\n", "").replaceAll("\t", "").replaceAll("\\s+", " "), 60)
  
  def defaultLibraries = {
    val classes = List(classOf[HCatLoader])
    classes.map(cl => try {
                  cl.getProtectionDomain().getCodeSource().getLocation().getFile
                } catch {
                  case t : Throwable => null
                })
           .filter(cl => cl != null && !"".equals(cl.trim))
  }
}

object PigTransformation {

  def scriptFrom(inputStream: InputStream): String = io.Source.fromInputStream(inputStream, "UTF-8").mkString

  def scriptFromResource(resourcePath: String): String = scriptFrom(getClass().getClassLoader().getResourceAsStream(resourcePath))

  def scriptFrom(filePath: String): String = scriptFrom(new FileInputStream(filePath))
}