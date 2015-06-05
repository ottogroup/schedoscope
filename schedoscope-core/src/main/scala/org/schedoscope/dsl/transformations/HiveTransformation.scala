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
import org.schedoscope.dsl.Transformation
import org.schedoscope.dsl.Version
import org.schedoscope.dsl.View
import scala.collection.JavaConversions._

case class HiveTransformation(sql: String, udfs: List[Function] = List()) extends Transformation {

  override def name = "hive"

  override def versionDigest = Version.digest(resourceHashes :+ sql)

  override def resources() = {
    udfs.flatMap(udf => udf.getResourceUris.map(uri => uri.getUri))
  }

  description = "[..]" + StringUtils.abbreviate(sql.replaceAll("\n", "").replaceAll("\t", "").replaceAll(".*SELECT", "SELECT").replaceAll("\\s+", " "), 60)
}

object HiveTransformation {

  def withFunctions(view: View, functions: Map[String, Class[_]] = Map()) = {
    val functionBuff = ListBuffer[Function]()

    for ((funcName, cls) <- functions) {
      val jarName = try {
        cls.getProtectionDomain().getCodeSource().getLocation().getFile.split("/").last
      } catch {
        case _: Throwable => null
      }

      val jarResources = Settings().getDriverSettings("hive").libJarsHdfs
        .filter(lj => jarName == null || lj.contains(jarName))
        .map(lj => new ResourceUri(ResourceType.JAR, lj))

      functionBuff.append(new Function(funcName, view.dbName, cls.getCanonicalName, null, null, 0, null, jarResources))
    }

    functionBuff.distinct.toList
  }

  def settingStatements(settings: Map[String, String] = Map()) = {
    val settingsStatements = new StringBuffer()

    for ((key, value) <- settings)
      settingsStatements.append(s"SET ${key}=${value};\n")

    settingsStatements.toString()
  }

  def insertStatement(view: View) = {
    val insertStatement = new StringBuffer()

    insertStatement
      .append("INSERT OVERWRITE TABLE ")
      .append(view.dbName)
      .append(".")
      .append(view.n)

    insertStatement.toString()
  }

  def insertInto(view: View, selectStatement: String, partition: Boolean = true, settings: Map[String, String] = Map()) = {
    val queryPrelude = new StringBuffer()

    queryPrelude
      .append(settingStatements(settings))
      .append(insertStatement(view))

    if (partition && view.partitionParameters.nonEmpty) {
      queryPrelude.append("\nPARTITION (")
      queryPrelude.append(view.partitionParameters.tail.foldLeft({ val first = view.partitionParameters.head; first.n + " = '" + first.v.get + "'" }) { (current, parameter) => current + ", " + parameter.n + " = '" + parameter.v.get + "'" })
      queryPrelude.append(")")
    }

    queryPrelude
      .append("\n")
      .append(selectStatement).toString()
  }

  def insertDynamicallyInto(view: View, selectStatement: String, settings: Map[String, String] = Map()) = {
    val queryPrelude = new StringBuffer()

    val augmentedSettings = new HashMap[String, String]() ++ settings
    augmentedSettings("hive.exec.dynamic.partition") = "true";
    augmentedSettings("hive.exec.dynamic.partition.mode") = "nonstrict";

    queryPrelude
      .append(settingStatements(augmentedSettings.toMap))
      .append(insertStatement(view))

    if (view.partitionParameters.nonEmpty) {
      queryPrelude.append("\nPARTITION (")
      queryPrelude.append(view.partitionParameters.tail.foldLeft(view.partitionParameters.head.n) { (current, parameter) => current + ", " + parameter.n })
      queryPrelude.append(")")
    }

    queryPrelude
      .append("\n")
      .append(selectStatement).toString()
  }

  def queryFrom(inputStream: InputStream): String = io.Source.fromInputStream(inputStream, "UTF-8").mkString

  def queryFromResource(resourcePath: String): String = queryFrom(getClass().getClassLoader().getResourceAsStream(resourcePath))

  def queryFrom(filePath: String): String = queryFrom(new FileInputStream(filePath))
}