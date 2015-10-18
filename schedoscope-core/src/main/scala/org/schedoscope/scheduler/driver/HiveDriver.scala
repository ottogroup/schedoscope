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
import org.schedoscope.dsl.transformations.HiveTransformation
import org.schedoscope.dsl.Transformation.replaceParameters
import org.slf4j.LoggerFactory
import HiveDriver.currentConnection

/**
 *
 */
class HiveDriver(val driverRunCompletionHandlerClassNames: List[String], val ugi: UserGroupInformation, val connectionUrl: String, val metastoreClient: HiveMetaStoreClient) extends Driver[HiveTransformation] {

  override def transformationName = "hive"

  implicit val executionContext = Settings().system.dispatchers.lookup("akka.actor.future-driver-dispatcher")

  val log = LoggerFactory.getLogger(classOf[HiveDriver])

  /**
   * @param t
   * @return
   */
  def run(t: HiveTransformation): DriverRunHandle[HiveTransformation] =
    new DriverRunHandle[HiveTransformation](this, new LocalDateTime(), t, future {
      t.udfs.foreach(this.registerFunction(_))
      executeHiveQuery(replaceParameters(t.sql, t.configuration.toMap))
    })

  /**
   * @param sql
   * @return
   */
  def executeHiveQuery(sql: String): DriverRunState[HiveTransformation] = {
    val queryStack = Stack[String]("")

    sql.split(";").map(el => {
      if (StringUtils.endsWith(queryStack.head, "\\")) {
        queryStack.push(StringUtils.chop(queryStack.pop()) + ";" + el)
      } else {
        queryStack.push(el)
      }
    })

    val queriesToExecute = queryStack.reverse.filter(q => !StringUtils.isBlank(q))

    queriesToExecute.foreach(
      q => try {
        statement.execute(q.trim())
      } catch {
        case e: SQLException => {
          if (e.getCause() != null && e.getCause().isInstanceOf[TProtocolException]) {
            cleanupResources
            throw DriverException(s"Runtime exception while executing Hive query ${q}", e.getCause())
          } else
            return DriverRunFailed[HiveTransformation](this, s"SQL exception while executing Hive query ${q}", e)

        }

        case t: Throwable => {
          cleanupResources
          throw DriverException(s"Runtime exception while executing Hive query ${q}", t)
        }
      })

    DriverRunSucceeded[HiveTransformation](this, s"Hive query ${sql} executed")
  }

  /**
   * @param f
   */
  def registerFunction(f: Function) {
    val existing = try {
      metastoreClient.getFunctions(f.getDbName, f.getFunctionName)
    } catch {
      case t: Throwable => throw DriverException(s"Runtime exception while executing registering function ${f}", t)
    }

    if (existing == null || existing.isEmpty()) {
      val resourceJars = f.getResourceUris.map(jar => s"JAR '${jar.getUri}'").mkString(", ")
      val createFunction = s"CREATE FUNCTION ${f.getDbName}.${f.getFunctionName} AS '${f.getClassName}' USING ${resourceJars}"

      this.executeHiveQuery(createFunction)
    }
  }

  /**
   * @return
   */
  private def connection = {
    if (currentConnection.get.isEmpty) {
      log.info("HIVE-DRIVER: Establishing connection to HiveServer")
      Class.forName(JDBC_CLASS)
      ugi.reloginFromTicketCache()
      currentConnection.set(Some(ugi.doAs(new PrivilegedAction[Connection]() {
        def run(): Connection = {
          DriverManager.getConnection(connectionUrl)
        }
      })))
    }

    currentConnection.get.get
  }

  private val currentStatement = new ThreadLocal[Option[Statement]]() {
    override def initialValue() = None
  }

  private def statement = {
    if (currentStatement.get.isEmpty) {
      log.info("HIVE-DRIVER: Creating statement on HiveServer")
      currentStatement.set(Some(connection.createStatement()))
    }
    currentStatement.get.get
  }

  private def cleanupResources() {
    try {
      if (currentStatement.get.isDefined) {
        log.warn("HIVE-DRIVER: Closing statement on HiveServer")
        currentStatement.get.get.close()
      }
    } catch {
      case _: Throwable =>
    } finally {
      currentStatement.set(None)
    }

    try {
      if (currentConnection.get.isDefined) {
        log.warn("HIVE-DRIVER: Closing connection to HiveServer")
        currentConnection.get.get.close()
      }
    } catch {
      case _: Throwable =>
    } finally {
      currentConnection.set(None)
    }
  }

  def JDBC_CLASS = "org.apache.hive.jdbc.HiveDriver"
}

/**
 *
 */
object HiveDriver {
  val currentConnection = new ThreadLocal[Option[Connection]]() {
    override def initialValue() = None
  }

  def apply(ds: DriverSettings) = {
    val ugi = Settings().userGroupInformation

    val conf = new HiveConf()
    conf.set("hive.metastore.local", "false");
    conf.setVar(HiveConf.ConfVars.METASTOREURIS, Settings().metastoreUri.trim());

    if (Settings().kerberosPrincipal.trim() != "") {
      conf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL,
        true);
      conf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL,
        Settings().kerberosPrincipal);
    }

    val metastoreClient = new HiveMetaStoreClient(conf)

    new HiveDriver(ds.driverRunCompletionHandlers, ugi, ds.url, metastoreClient)
  }
}

