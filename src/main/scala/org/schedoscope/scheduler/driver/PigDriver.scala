package org.schedoscope.scheduler.driver

import java.security.PrivilegedAction
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement

import scala.Array.canBuildFrom
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.Stack
import scala.concurrent.future

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Function
import org.apache.hadoop.security.UserGroupInformation
import org.apache.thrift.protocol.TProtocolException
import org.joda.time.LocalDateTime

import org.schedoscope.DriverSettings
import org.schedoscope.Settings
import org.schedoscope.dsl.transformations.PigTransformation
import org.schedoscope.dsl.Transformation.replaceParameters;
import org.schedoscope.scheduler.driver.HiveDriver.currentConnection;
import org.slf4j.LoggerFactory

import HiveDriver.currentConnection

import org.apache.pig.PigServer
import org.apache.pig.ExecType

import java.util.Properties

import org.apache.pig.PigException

import scala.collection.JavaConversions._

class PigDriver(val ugi: UserGroupInformation) extends Driver[PigTransformation] {

  override def transformationName = "pig"

  implicit val executionContext = Settings().system.dispatchers.lookup("akka.actor.future-driver-dispatcher")

  val log = LoggerFactory.getLogger(classOf[PigDriver])

  val driver = this // needed to access driver within ugi.doAs below

  def run(t: PigTransformation): DriverRunHandle[PigTransformation] =
    new DriverRunHandle[PigTransformation](this, new LocalDateTime(), t, future {
      // FIXME: future work: register jars, custom functions
      executePigScript(t.latin, t.configuration.toMap)
    })

  def executePigTransformation(latin: String, conf: Map[String, Any]): DriverRunState[PigTransformation] = {
    val actualLatin = replaceParameters(latin, conf)

    val props = new Properties()
    conf.foreach(c => props.put(c._1, c._2.asInstanceOf[Object]))
    
    ugi.doAs(new PrivilegedAction[DriverRunState[PigTransformation]]() {
      def run(): DriverRunState[PigTransformation] = {
        val ps = new PigServer(ExecType.valueOf(conf.getOrElse("exec.type", "MAPREDUCE").toString), props)
        try {
          // FIXME: we're doing parameter replacement by ourselves, because registerQuery doesn't support to specify parameters like 
          // registerScript does (and attention: pig doesn't support variable names containing a dot).
          // another workaround would be: ps.registerScript(IOUtils.toInputStream(latin, "UTF-8") , conf.filter(!_._1.contains(".")).map(c => (c._1, c._2.toString)))
          ps.registerQuery(actualLatin)
          DriverRunSucceeded[PigTransformation](driver, s"Pig script ${actualLatin} executed")
        } catch {
          // FIXME: do we need special handling for some exceptions here (similar to hive?)
          case e: PigException => DriverRunFailed(driver, s"PigException encountered while executing pig script ${actualLatin}", e)
          case t: Throwable => throw DriverException(s"Runtime exception while executing pig script ${actualLatin}", t)
        }
      }
    })

  }
}

object PigDriver {
  def apply(ds: DriverSettings) = {
    val ugi = Settings().userGroupInformation
    new PigDriver(ugi)
  }
}

