package com.ottogroup.bi.soda.crate
import com.ottogroup.bi.soda.crate.ddl.HiveQl
import com.ottogroup.bi.soda.dsl.View
import java.sql.DriverManager
import java.sql.Connection
import java.security.MessageDigest
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import scala.collection.mutable.HashSet
import scala.collection.JavaConversions._
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.fs.FileSystem
import java.security.PrivilegedAction
import scala.Array.canBuildFrom
import com.ottogroup.bi.soda.crate.ddl.HiveQl
import org.apache.hadoop.hive.metastore.api.Partition
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException
import org.apache.hadoop.hive.metastore.api.Database
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException
import java.io.InvalidObjectException
import com.ottogroup.bi.soda.bottler.api.Settings
import org.apache.hadoop.hive.metastore.api.Function
import org.joda.time.DateTime
import collection.JavaConversions._
import org.apache.hadoop.hive.metastore.api.MetaException
import scala.collection.mutable.HashMap
import com.ottogroup.bi.soda.dsl.TransformationVersion
import com.ottogroup.bi.soda.dsl.SchemaVersion
import com.ottogroup.bi.soda.dsl.Version

class DeploySchema(val metastoreClient: IMetaStoreClient, val connection: Connection) {
  val md5 = MessageDigest.getInstance("MD5")
  val existingSchemas = collection.mutable.Set[String]()

  def setTableProperty(dbName: String, tableName: String, key: String, value: String): Unit = {
    val table = metastoreClient.getTable(dbName, tableName)
    table.putToParameters(key, value)
    metastoreClient.alter_table(dbName, tableName, table)
  }

  def setPartitionProperty(dbName: String, tableName: String, part: String, key: String, value: String): Unit = {
    val partition = metastoreClient.getPartition(dbName, tableName, part)
    partition.putToParameters(key, value)
    metastoreClient.alter_partition(dbName, tableName, partition)
  }

  def setPartitionVersion(view: View) = {
    setPartitionProperty(view.dbName, view.n, view.partitionSpec, TransformationVersion.checksumProperty, view.transformation().versionDigest)
  }

  def getPartitionVersion(view: View): String = {
    try {
      val props = metastoreClient.getPartition(view.dbName, view.n, view.partitionSpec).getParameters()
      Version.check(props.get(TransformationVersion.checksumProperty))
    } catch {
      case e: Exception => throw e
    }
  }
  
  def dropAndCreateTableSchema(view: View): Unit = {
    val ddl = HiveQl.ddl(view)
    println("in dropAndCreateSchema " + view.dbName + "." + view.n + " " + ddl)
    val stmt = connection.createStatement()
    if (!metastoreClient.getAllDatabases.contains(view.dbName)) {
      stmt.execute(s"CREATE DATABASE ${view.dbName}")
    }
    if (metastoreClient.tableExists(view.dbName, view.n)) {
      metastoreClient.dropTable(view.dbName, view.n, false, true)
    }

    stmt.execute(ddl)

    setTableProperty(view.dbName, view.n, SchemaVersion.checksumProperty, Version.digest(ddl))
    println("!!created table " + ddl)
  }

  def schemaExists(view: View): Boolean = {
    val d = Version.digest(HiveQl.ddl(view))
    if (existingSchemas.contains(d))
      return true;
    if (!metastoreClient.tableExists(view.dbName, view.n)) {
      false
    } else {
      val table = metastoreClient.getTable(view.dbName, view.n)
      val props = table.getParameters()
      if (!props.containsKey(SchemaVersion.checksumProperty))
        false
      else if (d == props.get(SchemaVersion.checksumProperty).toString()) {
        existingSchemas += d
        true
      } else
        false
    }
  }

  def partitionExists(view: View): Boolean = {
    if (!schemaExists(view)) return false
    else
      try {
        metastoreClient.getPartition(view.dbName, view.n, view.partitionSpec)
      } catch {
        case e: NoSuchObjectException => return false
      }
    true
  }
  


  def createPartition(view: View): Partition = {
    if (!schemaExists(view)) {
      dropAndCreateTableSchema(view)
    }
    if (!view.isPartitioned())
      throw new RuntimeException(s"Cannot create partition on non-partitioned view ${view.tableName}")
    try {
      val now = new DateTime().getMillis.toInt
      val sd = metastoreClient.getTable(view.dbName, view.n).getSd
 
      sd.setLocation(view.fullPath)
      val part = new Partition(view.partitionValues, view.dbName, view.n, now, now, sd, HashMap[String, String]())
      metastoreClient.add_partitions(List(part), true, false)
      metastoreClient.getPartition(view.dbName, view.n, view.partitionSpec)
    } catch {
      case e: AlreadyExistsException => metastoreClient.getPartition(view.dbName, view.n, view.partitionSpec)
      case e: InvalidObjectException =>
        println(view.partitionSpec); throw (e)
      case e: MetaException => println(view.partitionSpec); throw (e)
    }

  }

  def removeObsoleteTables(dbname: String, validTables: List[String]) = {
    val tables = metastoreClient.getTables(dbname, "*")
    tables.diff(validTables).foreach { tableName =>
      {
        val table = metastoreClient.getTable(dbname, tableName)
        if (table.getParameters().containsKey(SchemaVersion.checksumProperty))
          metastoreClient.dropTable(dbname, tableName, false, true)
      }
    }
  }

  def deploySchemataForViews(views: Seq[View]): Unit = {
    val hashSet = HashSet[String]()
    views.filter(view => {
      if (hashSet.contains(HiveQl.ddl(view))) { false }
      else { hashSet.add(HiveQl.ddl(view)); true }
    }).foreach { view =>
      {
        if (!schemaExists(view))
          dropAndCreateTableSchema(view)
      }
    }
  }

}

object DeploySchema {
	def apply(jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val connection =
      Settings().userGroupInformation.doAs(new PrivilegedAction[Connection]() {
        def run(): Connection = {
          DriverManager.getConnection(jdbcUrl)
        }
      })

    val conf = new HiveConf()
    conf.set("hive.metastore.local", "false");
    conf.setVar(HiveConf.ConfVars.METASTOREURIS, metaStoreUri.trim());

    if (serverKerberosPrincipal.trim() != "") {
      conf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL,
        true);
      conf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL,
        serverKerberosPrincipal);
    }
    val metastoreClient = new HiveMetaStoreClient(conf)
    new DeploySchema(metastoreClient, connection)
  }

  def apply(metastoreClient: IMetaStoreClient, connection: Connection) = {
    new DeploySchema(metastoreClient, connection)
  }

  def main(args: Array[String]) = {

  }
}