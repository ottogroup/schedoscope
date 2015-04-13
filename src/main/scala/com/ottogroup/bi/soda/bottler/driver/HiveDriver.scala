package com.ottogroup.bi.soda.bottler.driver

import java.security.PrivilegedAction
import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager
import java.sql.SQLException

import scala.Array.canBuildFrom
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.Stack
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.concurrent.future

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Function
import org.apache.hadoop.security.UserGroupInformation
import org.joda.time.LocalDateTime

import com.ottogroup.bi.soda.DriverSettings
import com.ottogroup.bi.soda.Settings
import com.ottogroup.bi.soda.dsl.transformations.HiveTransformation
import com.ottogroup.bi.soda.dsl.transformations.HiveTransformation.replaceParameters

class HiveDriver(val ugi: UserGroupInformation, val connectionUrl: String, val metastoreClient: HiveMetaStoreClient) extends Driver[HiveTransformation] {

  override def transformationName = "hive"

  implicit val executionContext = Settings().system.dispatchers.lookup("akka.actor.future-driver-dispatcher")

  def run(t: HiveTransformation): DriverRunHandle[HiveTransformation] =
    new DriverRunHandle[HiveTransformation](this, new LocalDateTime(), t, future {
      t.udfs.foreach(this.registerFunction(_))
      executeHiveQuery(replaceParameters(t.sql, t.configuration.toMap))
    })

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

    val con = try {
      connection
    } catch {
      case t: Throwable => throw DriverException(s"Runtime exception while preparing Hive Server connection for query ${queriesToExecute}", t)
    }

    val stmt = try {
      con.createStatement()
    } catch {
      case e: SQLException => {
        closeConnection(con)
        return DriverRunFailed[HiveTransformation](this, s"SQL exception while preparing Hive query ${queriesToExecute}", e)
      }
      case t: Throwable => {
        closeConnection(con)
        throw DriverException(s"Runtime exception while preparing Hive query ${queriesToExecute}", t)
      }
    }

    queriesToExecute.foreach(
      q => try {
        stmt.execute(q.trim())
      } catch {
        case e: SQLException => {
          closeStatementAndConnection(con, stmt)
          return DriverRunFailed[HiveTransformation](this, s"SQL exception while executing Hive query ${q}", e)
        }

        case t: Throwable => {
          closeStatementAndConnection(con, stmt)
          throw DriverException(s"Runtime exception while executing Hive query ${q}", t)
        }
      })

    closeStatementAndConnection(con, stmt)

    DriverRunSucceeded[HiveTransformation](this, s"Hive query ${sql} executed")
  }

  def registerFunction(f: Function) {
    val existing = metastoreClient.getFunctions(f.getDbName, f.getFunctionName)
    if (existing == null || existing.size() == 0) {
      val resourceJars = f.getResourceUris.map(jar => s"JAR '${jar.getUri}'").mkString(", ")
      val createFunction = s"CREATE FUNCTION ${f.getDbName}.${f.getFunctionName} AS '${f.getClassName}' USING ${resourceJars}"

      this.executeHiveQuery(createFunction)
    }
  }

  private def connection = {
    Class.forName(JDBC_CLASS)
    ugi.reloginFromTicketCache()
    ugi.doAs(new PrivilegedAction[Connection]() {
      def run(): Connection = {
        DriverManager.getConnection(connectionUrl)
      }
    })
  }

  private def closeConnection(c: Connection) =
    try {
      c.close()
    } catch {
      case _: Throwable =>
    }

  private def closeStatement(s: Statement) =
    try {
      s.close()
    } catch {
      case _: Throwable =>
    }

  private def closeStatementAndConnection(c: Connection, s: Statement) {
    closeStatement(s)
    closeConnection(c)
  }

  def JDBC_CLASS = "org.apache.hive.jdbc.HiveDriver"
}

object HiveDriver {
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

    new HiveDriver(ugi, ds.url, metastoreClient)
  }
}

