package com.ottogroup.bi.soda.dsl.transformations.oozie

import com.ottogroup.bi.soda.dsl.Transformation
import java.util.Properties
import java.io.FileReader
import java.io.InputStream
import com.typesafe.config.ConfigFactory
import collection.JavaConversions._
import org.apache.oozie.client.OozieClient
import java.io.FileInputStream
import org.apache.commons.lang.StringUtils
import com.ottogroup.bi.soda.dsl.NamedTransformation

case class OozieTransformation(bundle: String, workflow: String, workflowAppPath: String, c: Map[String, String]) extends Transformation {
  configureWith(c)

  override def resources() = {
    List(workflowAppPath)
  }

  description = StringUtils.abbreviate(s"${bundle}/${workflow}", 100)
}

object OozieTransformation extends NamedTransformation {
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