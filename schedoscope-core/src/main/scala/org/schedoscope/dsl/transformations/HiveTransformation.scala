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

import java.io.{FileInputStream, InputStream}

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hive.metastore.api.{Function, ResourceType, ResourceUri}
import org.schedoscope.Settings
import org.schedoscope.dsl.View

import scala.collection.JavaConversions._
import scala.collection.mutable.{HashMap, ListBuffer}

/**
  * Hive Transformation: compute views via HiveQL Hive Server 2.
  *
  */
case class HiveTransformation(sql: String, udfs: List[Function] = List()) extends Transformation {

  import HiveTransformation._

  def name = "hive"

  override def fileResourcesToChecksum =
    udfs.flatMap(udf => udf.getResourceUris.map(uri => uri.getUri))

  override def stringsToChecksum = List(normalizeQuery(sql))

  description = "[..]" + StringUtils.abbreviate(sql.replaceAll("\n", "").replaceAll("\t", "").replaceAll(".*SELECT", "SELECT").replaceAll("\\s+", " "), 60)

  @throws[InvalidTransformationException]
  override def validateTransformation() =
    if (!checkJoinsWithOns(sql)) {
      throw new InvalidTransformationException("Uneven count of joins and ons in Hive query")
    }


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
      queryPrelude.append(view.partitionParameters.tail.foldLeft({
        val first = view.partitionParameters.head
        first.n + " = '" + first.v.get + "'"
      }) { (current, parameter) => current + ", " + parameter.n + " = '" + parameter.v.get + "'" })
      queryPrelude.append(")")
    }

    queryPrelude
      .append("\n")
      .append(selectStatement).toString()
  }

  def insertDynamicallyInto(view: View, selectStatement: String, settings: Map[String, String] = Map()) = {
    val queryPrelude = new StringBuffer()

    val augmentedSettings = new HashMap[String, String]() ++ settings
    augmentedSettings("hive.exec.dynamic.partition") = "true"
    augmentedSettings("hive.exec.dynamic.partition.mode") = "nonstrict"

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

  def checkJoinsWithOns(sql: String): Boolean = {
    val normalizedSQl = normalizeQuery(sql)
    // delete empty strings
    val noEmptyStrings = normalizedSQl.replaceAll("(''|\"\")","")
    // The expression will delete the characters between two quotes:
    // ((?<![\\])['"])  Match a single or double quote, as long as it's not preceded by \
    // (['"]) store the matched quote
    // (?:.(?!\1))*.? Continue matching ANY characters.. as long as they aren't followed by the same quote that was matched in #1...
    val noStrings = noEmptyStrings.replaceAll("((?<![\\\\])['\"])((?:.(?!(?<![\\\\])\\1))*.?)\\1", "")
    // prepend/append spaces to make the count easier
    val paddedSQL = " " + noStrings.replaceAll("[(),]"," ") + " "

    val join = "(?i)(?<! cross) join(?= )".r
    val on = "(?i) on(?= )".r
    val countJoins = join.findAllIn(paddedSQL).length
    val countOns = on.findAllIn(paddedSQL).length

    countJoins == countOns
  }

  def queryFrom(inputStream: InputStream): String = scala.io.Source.fromInputStream(inputStream, "UTF-8").mkString

  def queryFromResource(resourcePath: String): String = queryFrom(getClass().getClassLoader().getResourceAsStream(resourcePath))

  def queryFrom(filePath: String): String = queryFrom(new FileInputStream(filePath))

  def normalizeQuery(sql: String): String = {
    val replaceDQ = HiveTransformation.replaceWhitespacesBetweenChars("\"") _
    val replaceSQ = HiveTransformation.replaceWhitespacesBetweenChars("'") _
    val replaceWhitespace = (s: String) => replaceSQ(replaceDQ(s))

    val sqlLines = sql.split("\n")

    val noComments = sqlLines.map {
      _.trim().replaceAll("^--(.|\\w)*$", "")
    }.filter(_.nonEmpty).mkString(" ")

    val noSets = noComments.replaceAll("(?i)( |^)SET [^;]+;","")

    //replace the whitespaces inside quotes with ;
    replaceWhitespace(noSets)
      // replace multiple whitespaces and tabs with a single space
      .replaceAll("\\s+", " ")
      .trim
  }

  def replaceWhitespacesBetweenChars(quote: String)(string: String): String = {
    val endsWithQuote = string.endsWith(quote) && !string.endsWith("\\" + quote)

    val array = string
      .split("(?<!\\\\)" + quote)

    if (array.length <= 1) {
      //no pair of characters nothing to do here
      return string
    }

    val replaced = array
      .zipWithIndex
      .map { case (s, i) =>
        //make sure the char has a twin before replacing
        if (i % 2 == 1 && (i < array.length - 1 || endsWithQuote)) {
          val nested = if(quote == "\"") s.replaceAll("'","\"") else s
          nested.replaceAll("\\s", ";")
        } else {
          s
        }
      }.mkString(quote)

    //append ending quote if there was one in the input string
    if (endsWithQuote) {
      replaced + quote
    } else {
      replaced
    }
  }


}
