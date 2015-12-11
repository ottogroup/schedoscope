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
import org.schedoscope.Settings
import org.schedoscope.dsl.View
import scala.collection.JavaConversions._
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException
import org.apache.hadoop.hive.ql.udf.UDFLength
import org.apache.hive.hcatalog.pig.HCatLoader
import org.apache.hive.hcatalog.data.schema.HCatSchema
import parquet.pig.ParquetStorer
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege

/**
 * Pig Transformation - Compute a view from a shell script.
 *
 * @param latin Pig script to execute
 * @param dirsToDelete List of directories to empty before Pig execution. Does not include the view's fullPath!
 *
 */
case class PigTransformation(latin: String, dirsToDelete: List[String] = List()) extends Transformation {

  override def name = "pig"

  override def stringsToChecksum = List(latin)

  description = "[..]" + StringUtils.abbreviate(latin.replaceAll("\n", "").replaceAll("\t", "").replaceAll("\\s+", " "), 60)

  def defaultLibraries = {
    // FIXME: declare jars instead of any random class included in this jar
    val classes = List(
      // needed for usage of HCatalog table management
      classOf[HCatLoader], classOf[HCatSchema], classOf[HiveObjectPrivilege], classOf[UDFLength],
      // needed for usage of storage format Parquet with pig
      classOf[ParquetStorer])
    classes.map(cl => try {
      cl.getProtectionDomain().getCodeSource().getLocation().getFile
    } catch {
      case t: Throwable => null
    })
      .filter(cl => cl != null && !"".equals(cl.trim))
  }
}

object PigTransformation {

  def scriptFrom(inputStream: InputStream): String = scala.io.Source.fromInputStream(inputStream, "UTF-8").mkString

  def scriptFromResource(resourcePath: String): String = scriptFrom(getClass().getClassLoader().getResourceAsStream(resourcePath))

  def scriptFrom(filePath: String): String = scriptFrom(new FileInputStream(filePath))
}
