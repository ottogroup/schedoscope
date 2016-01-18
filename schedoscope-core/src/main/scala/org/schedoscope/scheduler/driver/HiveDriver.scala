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

import java.sql.{ Connection, DriverManager, SQLException, Statement }
import java.net.ConnectException
import java.security.PrivilegedAction

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Function
import org.apache.hadoop.security.UserGroupInformation
import org.apache.thrift.TException
import org.joda.time.LocalDateTime
import org.schedoscope.{ DriverSettings, Schedoscope }
import org.schedoscope.dsl.transformations.HiveTransformation
import org.schedoscope.dsl.transformations.Transformation.replaceParameters
import org.schedoscope.scheduler.driver.HiveDriver.currentConnection
import org.slf4j.LoggerFactory

import scala.Array.canBuildFrom
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.Stack
import scala.concurrent.Future

/**
 * Driver for executing Hive transformations
 */
class HiveDriver(val driverRunCompletionHandlerClassNames: List[String], val ugi: UserGroupInformation, val connectionUrl: String, val metastoreClient: HiveMetaStoreClient) extends Driver[HiveTransformation] {

  /**
   * Set transformation name to hive
   */
  override def transformationName = "hive"

  val log = LoggerFactory.getLogger(classOf[HiveDriver])

  def isConnectionProblem(t: Throwable): Boolean = {
    val result = t.isInstanceOf[TException] || t.isInstanceOf[ConnectException]

    if (!result && t.getCause() != null) {
      isConnectionProblem(t.getCause())
    } else result
  }

  /**
   * Construct a future-based driver run handle
   */
  def run(t: HiveTransformation): DriverRunHandle[HiveTransformation] =
    new DriverRunHandle[HiveTransformation](this, new LocalDateTime(), t, Future {
      t.udfs.foreach(this.registerFunction(_))
      executeHiveQuery(replaceParameters(t.sql, t.configuration.toMap))
    })

  /**
   * Actually perform the given query and return a run state after completion.
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
        case t: Throwable => {
          cleanupResources
          if (isConnectionProblem(t))
            throw RetryableDriverException(s"Hive driver encountered connection problem while executing hive query ${q}", t)
          else
            return DriverRunFailed[HiveTransformation](this, s"Unknown exception caught while executing Hive query ${q}. Failing run.", t)
        }
      })

    DriverRunSucceeded[HiveTransformation](this, s"Hive query ${sql} executed")
  }

  def registerFunction(f: Function) {
    val existing = try {
      metastoreClient.getFunctions(f.getDbName, f.getFunctionName)
    } catch {
      case t: Throwable => throw RetryableDriverException(s"Runtime exception while executing registering function ${f}", t)
    }

    if (existing == null || existing.isEmpty()) {
      val resourceJars = f.getResourceUris.map(jar => s"JAR '${jar.getUri}'").mkString(", ")
      val createFunction = s"CREATE FUNCTION ${f.getDbName}.${f.getFunctionName} AS '${f.getClassName}' USING ${resourceJars}"

      this.executeHiveQuery(createFunction)
    }
  }

  /**
   * We need to handle connections and statements as ThreadLocals because the Hive JDBC driver
   * binds sessions to threads :-(
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

  /**
   * We need to cleanup  connections and statements and their ThreadLocals because the Hive JDBC driver
   * binds sessions to threads :-(
   */
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
 * Factory methods for Hive drivers
 */
object HiveDriver {
  val currentConnection = new ThreadLocal[Option[Connection]]() {
    override def initialValue() = None
  }

  def apply(ds: DriverSettings) = {
    val ugi = Schedoscope.settings.userGroupInformation

    val conf = new HiveConf()
    conf.set("hive.metastore.local", "false");
    conf.setVar(HiveConf.ConfVars.METASTOREURIS, Schedoscope.settings.metastoreUri.trim());

    if (Schedoscope.settings.kerberosPrincipal.trim() != "") {
      conf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL,
        true);
      conf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL,
        Schedoscope.settings.kerberosPrincipal);
    }

    val metastoreClient = new HiveMetaStoreClient(conf)

    new HiveDriver(ds.driverRunCompletionHandlers, ugi, ds.url, metastoreClient)
  }
}

