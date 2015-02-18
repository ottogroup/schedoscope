package com.ottogroup.bi.soda.bottler.driver
import com.ottogroup.bi.soda.dsl.transformations.sql.HiveQl
import com.ottogroup.bi.soda.dsl.transformations.sql.HiveQl._
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

class HiveDriver(conn: Connection) extends Driver {

  val connection = conn

  override def run(t: Transformation): String = {
    t match {
      case th: HiveQl =>
        th.sql.map(sql => replaceParameters(sql, th.configuration.toMap))
              .map(sql => if (!this.executeHiveQuery(sql)) return "")
      case _ => throw new RuntimeException("HiveDriver can only run HiveQl transformations.")
    }
    ""
  }

  override def runAndWait(t: Transformation): Boolean = {
    t match {
      case th: HiveQl =>
        th.sql.map(sql => replaceParameters(sql, th.configuration.toMap))
          .map(sql => if (!this.executeHiveQuery(sql)) return false)
      case _ => throw new RuntimeException("HiveDriver can only run HiveQl transformations.")
    }
    true
  }

  def executeHiveQuery(sql: String): Boolean = {
    if (sql == null)
      return false

    // we mimic here the simple sql query splitting from
    // org.apache.hadoop.hive.cli.CliDriver#processLine    
    println(sql)
    val queries = Stack[String]("")
    sql.split(";").map(el => {
      if (StringUtils.endsWith(queries.head, "\\")) {
        queries.push(StringUtils.chop(queries.pop()) + ";" + el)
      } else {
        queries.push(el)
      }
    })
    queries.reverse.filter(q => !StringUtils.isBlank(q))
      .map(q => {
        val stmt = connection.createStatement()
        stmt.execute(q)
      })
    true
  }
}

object HiveDriver {
  def apply(ds:DriverSettings) = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val c =
      Settings().userGroupInformation.doAs(new PrivilegedAction[Connection]() {
        def run(): Connection = {
          DriverManager.getConnection(Settings().jdbcUrl)
        }
      })
    val hd = new HiveDriver(c)
    hd.driverSettings = ds
    hd
  }
}

