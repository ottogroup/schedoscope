package com.ottogroup.bi.soda.crate

import java.security.MessageDigest
import java.security.PrivilegedAction
import java.sql.Connection
import java.sql.DriverManager

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.JavaConversions.mutableMapAsJavaMap
import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException
import org.apache.hadoop.hive.metastore.api.Partition
import org.joda.time.DateTime

import com.ottogroup.bi.soda.Settings
import com.ottogroup.bi.soda.crate.ddl.HiveQl
import com.ottogroup.bi.soda.dsl.Version
import com.ottogroup.bi.soda.dsl.View

class SchemaManager(val metastoreClient: IMetaStoreClient, val connection: Connection) {
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

  def setTransformationVersion(view: View) = {
    setPartitionProperty(view.dbName, view.n, view.partitionSpec, Version.TransformationVersion.checksumProperty, view.transformation().versionDigest)
  }

  def getTransformationVersions(dbName: String, tableName: String) = {
    val parts = metastoreClient.listPartitions(dbName, tableName, Short.MaxValue)
    new HashMap[String, String]() ++= parts.map(p => {      
      (p.getValues.mkString("/") -> p.getParameters.getOrElse(Version.TransformationVersion.checksumProperty, Version.default))
    }).toMap
  }

  def setTransformationTimestamp(view: View, timestamp: Long) = {
    setPartitionProperty(view.dbName, view.n, view.partitionSpec, Version.TransformationVersion.timestampProperty, timestamp.toString)
  }

  def getTransformationTimestamps(dbName: String, tableName: String) = {
    val parts = metastoreClient.listPartitions(dbName, tableName, Short.MaxValue)
    new HashMap[String, Long]() ++= parts.map(p => {
      val params = p.getParameters
      (p.getValues.mkString("/") -> p.getParameters.getOrElse(Version.TransformationVersion.timestampProperty, "0").toLong)
    }).toMap
  }


  def dropAndCreateTableSchema(view: View): Unit = {
    val ddl = HiveQl.ddl(view)
    val stmt = connection.createStatement()
    if (!metastoreClient.getAllDatabases.contains(view.dbName)) {
      stmt.execute(s"CREATE DATABASE ${view.dbName}")
    }
    if (metastoreClient.tableExists(view.dbName, view.n)) {
      metastoreClient.dropTable(view.dbName, view.n, false, true)
    }

    stmt.execute(ddl)
    stmt.close()

    setTableProperty(view.dbName, view.n, Version.SchemaVersion.checksumProperty, Version.digest(ddl))
  }

  def schemaExists(view: View): Boolean = {
    val d = Version.digest(HiveQl.ddl(view))

    if (existingSchemas.contains(d))
      return true

    if (!metastoreClient.tableExists(view.dbName, view.n))
      false
    else {
      val table = metastoreClient.getTable(view.dbName, view.n)

      val props = table.getParameters()
      if (!props.containsKey(Version.SchemaVersion.checksumProperty))
        false
      else if (d == props.get(Version.SchemaVersion.checksumProperty).toString()) {
        existingSchemas += d
        true
      } else
        false
    }
  }

  def partitionExists(view: View): Boolean = {
    if (!schemaExists(view))
      false
    else
      try {
        metastoreClient.getPartition(view.dbName, view.n, view.partitionSpec)
        true
      } catch {
        case e: NoSuchObjectException => false
      }
  }

  def createPartitions(views: List[View]): List[Partition] = {
    if (views.size == 0)
      List()
    else {
      val tablePrototype = views.head

      if (!schemaExists(tablePrototype)) {
        dropAndCreateTableSchema(tablePrototype)
      }

      val sd = metastoreClient.getTable(tablePrototype.dbName, tablePrototype.n).getSd

      val partitions = views
        .filter { _.isPartitioned }
        .map({ v =>
          val now = new DateTime().getMillis.toInt
          sd.setLocation(v.fullPath)

          new Partition(v.partitionValues, v.dbName, v.n, now, now, sd, HashMap[String, String]())
        })

      try {
        metastoreClient.add_partitions(partitions, true, false)
        partitions
      } catch {
        case e: AlreadyExistsException => partitions
        case t: Throwable => throw t
      }
    }
  }

  def createPartition(view: View): Partition = createPartitions(List(view)).headOption.getOrElse(null)

  def deploySchemataForViews(views: Seq[View]): Unit = {
    val hashSet = HashSet[String]()
    views.filter(view =>
      if (hashSet.contains(HiveQl.ddl(view)))
        false
      else {
        hashSet.add(HiveQl.ddl(view))
        true
      }).foreach { view =>
      {
        if (!schemaExists(view))
          dropAndCreateTableSchema(view)
      }
    }
  }
}

object SchemaManager {
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
    new SchemaManager(metastoreClient, connection)
  }

  def apply(metastoreClient: IMetaStoreClient, connection: Connection) = {
    new SchemaManager(metastoreClient, connection)
  }
}