package com.ottogroup.bi.soda.bottler.driver

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
import com.ottogroup.bi.soda.DriverSettings
import com.ottogroup.bi.soda.Settings
import com.ottogroup.bi.soda.dsl.transformations.PigTransformation
import org.slf4j.LoggerFactory
import HiveDriver.currentConnection
import org.apache.pig.PigServer
import org.apache.pig.ExecType
import java.util.Properties
import org.apache.pig.PigException
import scala.collection.JavaConversions._
import com.ottogroup.bi.soda.dsl.transformations.HiveTransformation

class PigDriver(val ugi: UserGroupInformation) extends Driver[PigTransformation] {

  override def transformationName = "pig"

  implicit val executionContext = Settings().system.dispatchers.lookup("akka.actor.future-driver-dispatcher")

  val log = LoggerFactory.getLogger(classOf[PigDriver])

  def run(t: PigTransformation): DriverRunHandle[PigTransformation] =
    new DriverRunHandle[PigTransformation](this, new LocalDateTime(), t, future {
      // FIXME: future work: register jars, custom functions
      executePigScript(t.latin, t.configuration.toMap)
    })

  def executePigScript(latin: String, conf: Map[String, Any]): DriverRunState[PigTransformation] = {
    val actualLatin = HiveTransformation.replaceParameters(latin, conf)

    val props = new Properties()
    conf.foreach(c => props.put(c._1, c._2.asInstanceOf[Object]))

    val thisDriver = this
    ugi.doAs(new PrivilegedAction[DriverRunState[PigTransformation]]() {
      def run(): DriverRunState[PigTransformation] = {
        val ps = new PigServer(ExecType.valueOf(conf.getOrElse("exec.type", "LOCAL").toString), props)
        try {
          ps.registerQuery(actualLatin)
          DriverRunSucceeded[PigTransformation](thisDriver, s"Pig script ${actualLatin} executed")
        } catch {
          // FIXME: do we need special handling for some exceptions here (similar to hive?)
          case e: PigException => DriverRunFailed(thisDriver, s"PigException encountered while executing pig script ${actualLatin}", e)
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

