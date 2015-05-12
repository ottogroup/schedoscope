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

case class MapreduceTransformation(createJob: (Map[String,Any]) => Job, c: Map[String, Any]) extends Transformation {
  
  configureWith(c)
  
  override def name = "mapreduce"
  
  lazy val job = createJob(configuration.toMap) 

  override def versionDigest = Version.digest("") // TODO

  description = StringUtils.abbreviate(createJob(configuration.toMap).getJobName, 100) 

}

object MapreduceTransformation {
 
}

trait MapreduceJobFactory {
  
  @throws(classOf[Exception])
  def create(args: Array[String]) : Job 
  
}