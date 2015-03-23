package com.ottogroup.bi.soda.dsl.transformations

import java.io.FileInputStream
import java.io.InputStream
import java.util.Properties
import org.apache.commons.lang.StringUtils
import com.ottogroup.bi.soda.Settings
import com.ottogroup.bi.soda.dsl.Transformation
import scala.collection.JavaConversions._

case class OozieTransformation(bundle: String, workflow: String, workflowAppPath: String, c: Map[String, String]) extends Transformation {
  configureWith(c)

  override def name = "oozie"
  
  override def resources() = {
    List(workflowAppPath)
  }
  
  description = StringUtils.abbreviate(s"${bundle}/${workflow}", 100)
}

object OozieTransformation {
  def oozieWFPath(env: String, bundle: String, workflow: String) = s"${Settings().getDriverSettings("oozie").location}/workflows/${bundle}/${workflow}/"

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