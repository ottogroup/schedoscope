package com.ottogroup.bi.soda.dsl.transformations

import java.io.FileInputStream
import java.io.InputStream
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hive.metastore.api.Function
import org.apache.hadoop.hive.metastore.api.ResourceType
import org.apache.hadoop.hive.metastore.api.ResourceUri
import com.ottogroup.bi.soda.Settings
import com.ottogroup.bi.soda.dsl.Transformation
import com.ottogroup.bi.soda.dsl.Version
import com.ottogroup.bi.soda.dsl.View
import scala.collection.JavaConversions._
import org.apache.hadoop.mapreduce.Job

case class MapreduceTransformation(factory: MapreduceJobFactory, c: Map[String, Object]) extends Transformation {
  
  configureWith(c)
  
  lazy val job = factory.create(configuration.getOrElse("arguments", Array()).asInstanceOf[Array[String]])
  
  override def name = "mapreduce"

  override def versionDigest = Version.digest("") // TODO

  description = StringUtils.abbreviate("jobname", 100) // FIXME: lazy val job.. 
}

object MapreduceTransformation {
  
  def fromFactory[P <: MapreduceJobFactory](jobFactoryClass: Class[P])  = jobFactoryClass.newInstance()

}

trait MapreduceJobFactory {
  
  @throws(classOf[Exception])
  def create(args: Array[String]) : Job 
  
}