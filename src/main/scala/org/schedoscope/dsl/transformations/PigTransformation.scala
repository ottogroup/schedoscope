package org.schedoscope.dsl.transformations

import java.io.FileInputStream
import java.io.InputStream

import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hive.metastore.api.Function
import org.apache.hadoop.hive.metastore.api.ResourceType
import org.apache.hadoop.hive.metastore.api.ResourceUri
import org.schedoscope.dsl.Version;

import org.schedoscope.Settings
import org.schedoscope.dsl.Transformation
import org.schedoscope.dsl.View
import scala.collection.JavaConversions._

case class PigTransformation(latin: String, directoriesToDelete: List[String], c: Map[String, String]) extends Transformation {

  override def name = "pig"

  override def versionDigest = Version.digest(latin)

  description = "[..]" + StringUtils.abbreviate(latin.replaceAll("\n", "").replaceAll("\t", "").replaceAll("\\s+", " "), 60)

  configureWith(c)
}

object PigTransformation {

  def scriptFrom(inputStream: InputStream): String = io.Source.fromInputStream(inputStream, "UTF-8").mkString

  def scriptFromResource(resourcePath: String): String = scriptFrom(getClass().getClassLoader().getResourceAsStream(resourcePath))

  def scriptFrom(filePath: String): String = scriptFrom(new FileInputStream(filePath))
}