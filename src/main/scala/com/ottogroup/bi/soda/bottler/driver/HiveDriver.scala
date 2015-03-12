package com.ottogroup.bi.soda.bottler.driver
import com.ottogroup.bi.soda.dsl.transformations.sql.HiveTransformation
import com.ottogroup.bi.soda.dsl.transformations.sql.HiveTransformation._
import util.control.Breaks._
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.security.PrivilegedAction
import java.sql.Connection
import java.sql.DriverManager
import org.apache.commons.lang.StringUtils
import scala.collection.mutable.MutableList
import scala.collection.mutable.Queue
import scala.collection.mutable.Stack
import com.ottogroup.bi.soda.dsl.Transformation
import com.typesafe.config.Config
import com.ottogroup.bi.soda.bottler.api.Settings
import com.ottogroup.bi.soda.bottler.api.DriverSettings
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.Function
import collection.JavaConversions._
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException
import org.apache.hadoop.hive.metastore.api.MetaException
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException
import org.apache.thrift.transport.TTransportException
import scala.concurrent.duration.Duration
import scala.concurrent._
import org.joda.time.LocalDateTime
import org.apache.thrift.TException
import java.sql.SQLException

class HiveDriver(val ugi: UserGroupInformation, val connectionUrl: String, val metastoreClient: HiveMetaStoreClient) extends Driver[HiveTransformation] {

  override def runTimeOut: Duration = Settings().hiveActionTimeout

  def run(t: HiveTransformation): DriverRunHandle[HiveTransformation] =
    new DriverRunHandle[HiveTransformation](this, new LocalDateTime(), t, null, future {
      t.udfs.foreach(this.registerFunction(_))
      executeHiveQuery(replaceParameters(t.sql, t.configuration.toMap))
    }(ExecutionContext.global))

  def executeHiveQuery(sql: String): DriverRunState[HiveTransformation] = {
    println(sql)

    val queryStack = Stack[String]("")

    sql.split(";").map(el => {
      if (StringUtils.endsWith(queryStack.head, "\\")) {
        queryStack.push(StringUtils.chop(queryStack.pop()) + ";" + el)
      } else {
        queryStack.push(el)
      }
    })

    val queriesToExecute = queryStack.reverse.filter(q => !StringUtils.isBlank(q))
    val stmt = connection.createStatement()
    
    queriesToExecute.foreach(
      q => try {        
        stmt.execute(q.trim())
      } catch {
        case e: SQLException =>
          return DriverRunFailed[HiveTransformation](this, s"SQL exception while executing Hive query ${q}", e)
        case t: Throwable => throw DriverException(s"Runtime exception while executing Hive query ${q}", t)
      })
    stmt.close()

    DriverRunSucceeded[HiveTransformation](this, s"Hive query ${sql} executed")
  }

  def registerFunction(f: Function) {
    val existing = metastoreClient.getFunctions(f.getDbName, f.getFunctionName)
    if (existing == null || existing.size() == 0) {
      val resourceJars = f.getResourceUris.map(jar => s"JAR '${jar.getUri}'").mkString(", ")
      val createFunction = s"CREATE FUNCTION ${f.getDbName}.${f.getFunctionName} AS '${f.getClassName}' USING ${resourceJars}"

      println(createFunction)

      this.executeHiveQuery(createFunction)
    }
  }

  def connection = {
    Class.forName(JDBC_CLASS)
    ugi.reloginFromTicketCache()
    ugi.doAs(new PrivilegedAction[Connection]() {
      def run(): Connection = {
        DriverManager.getConnection(connectionUrl)
      }
    })
  }

  def JDBC_CLASS = "org.apache.hive.jdbc.HiveDriver"
}

object HiveDriver extends NamedDriver {
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

