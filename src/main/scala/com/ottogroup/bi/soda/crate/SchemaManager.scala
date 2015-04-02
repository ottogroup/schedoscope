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

  def getPartitionKey(viewOrPartition: Any) = viewOrPartition match {
    case v: View      => if (v.isPartitioned()) v.partitionValues.mkString("/") else "no-partition"
    case p: Partition => p.getValues.mkString("/")
    case _            => throw new RuntimeException("Cannot create partition key for " + viewOrPartition)
  }

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
    if (view.isPartitioned()) {
      setPartitionProperty(view.dbName, view.n, view.partitionSpec, Version.TransformationVersion.checksumProperty, view.transformation().versionDigest)
    } else {
      setTableProperty(view.dbName, view.n, Version.TransformationVersion.checksumProperty, view.transformation().versionDigest)
    }
  }

  def setTransformationTimestamp(view: View, timestamp: Long) = {
    if (view.isPartitioned()) {
      setPartitionProperty(view.dbName, view.n, view.partitionSpec, Version.TransformationVersion.timestampProperty, timestamp.toString)
    } else {
      setTableProperty(view.dbName, view.n, Version.TransformationVersion.timestampProperty, timestamp.toString)
    }
  }

  def getTransformationVersions(view: View) = {
    HashMap[String, String]() ++= getTransformationMetadata(List(view)).map { case (view, (version, _)) => (getPartitionKey(view), version) }
  }

  def getTransformationTimestamps(view: View) = {
    HashMap[String, Long]() ++= getTransformationMetadata(List(view)).map { case (view, (_, timeStamp)) => (getPartitionKey(view), timeStamp) }
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

  def createNonexistingPartitions(tablePrototype: View, partitions: List[Partition]): Map[View, (String, Long)] = {
    if (partitions.size == 0 || !tablePrototype.isPartitioned()) {
      return Map()
    }
    try {
      metastoreClient.add_partitions(partitions, false, false)
      //println(s"[METASTORE] returned ${res.size} of ${partitions.size}. Exemplary metadata check: ${if (res.size > 0) res.head.getParameters.size() else "n/a"} params")
      partitions.map(p => (partitionToView(tablePrototype, p) -> (Version.default, 0.toLong))).toMap
    } catch {
      case t: Throwable => throw t
    }
  }

  def getTransformationMetadata(views: List[View]): Map[View, (String, Long)] = {
    val tablePrototype = views.head
    if (!schemaExists(tablePrototype)) {
      dropAndCreateTableSchema(tablePrototype)
    }
    if (tablePrototype.isPartitioned()) {
      val existingPartitionNames = metastoreClient.listPartitionNames(tablePrototype.dbName, tablePrototype.n, -1).map(n => "/" + n).toSet

      //println(s"Existing partitions for ${tablePrototype.tableName} : ${existingPartitionNames}")

      val existingPartitions = viewsToPartitions(views.filter(v => existingPartitionNames.contains(v.partitionSpec)))
      val nonExistingPartitions = viewsToPartitions(views.filter(v => !existingPartitionNames.contains(v.partitionSpec)))

      println(s"existing: ${existingPartitions.size}, non-existing: ${nonExistingPartitions.size}")

      val existingMetadata = getExistingTransformationMetadata(tablePrototype, existingPartitions)
      val createdMetadata = nonExistingPartitions.grouped(Settings().metastoreBatchSize).map(nep => {
        println(s"Creating ${nep.size} partitions for ${tablePrototype.tableName}")
        createNonexistingPartitions(tablePrototype, nep.values.toList)
      }).reduceOption(_ ++ _).getOrElse(Map())

      existingMetadata ++ createdMetadata
    } else getExistingTransformationMetadata(tablePrototype, Map[String, Partition]())
  }

  def getExistingTransformationMetadata(tablePrototype: View, partitions: Map[String, Partition]): Map[View, (String, Long)] = {
    if (tablePrototype.isPartitioned) {
      println(s"fetching by partition values ${partitions.map(p => p._1).toList} for table ${tablePrototype.tableName}")
      val existingPartitions = metastoreClient.getPartitionsByNames(tablePrototype.dbName, tablePrototype.n, partitions.map(p => p._1).toList) // FIME: is this the correct part_val?
      println(s" got ${existingPartitions.size()} results")
      existingPartitions.map { p =>
        {
          val partitionMetadataForView = p.getParameters
          (partitionToView(tablePrototype, p), (partitionMetadataForView.getOrElse(Version.TransformationVersion.checksumProperty, Version.default), partitionMetadataForView.getOrElse(Version.TransformationVersion.timestampProperty, "0").toLong))
        }
      }.toMap
    } else {
      val tableMetadata = metastoreClient.getTable(tablePrototype.dbName, tablePrototype.n).getParameters
      Map((tablePrototype, (tableMetadata.getOrElse(Version.TransformationVersion.checksumProperty, Version.default), tableMetadata.getOrElse(Version.TransformationVersion.timestampProperty, "0").toLong)))
    }
  }

  def partitionToView(tablePrototype: View, p: Partition) = {
    View.viewsFromUrl(Settings().env, s"${tablePrototype.urlPathPrefix}${p.getValues.mkString("/")}", Settings().viewAugmentor).head
  }

  def viewsToPartitions(views: List[View]): Map[String, Partition] = {
    if (views.size == 0) {
      return Map()
    }
    val tablePrototype = views.head
    val sd = metastoreClient.getTable(tablePrototype.dbName, tablePrototype.n).getSd
    views.map({ v =>
      val now = new DateTime().getMillis.toInt
      sd.setLocation(v.fullPath)
      (v.partitionSpec.replaceFirst("/", "") -> new Partition(v.partitionValues, v.dbName, v.n, now, now, sd, HashMap[String, String]()))
    }).toMap
  }

  def createPartition(view: View): Partition = null
  //((createNonexistingPartitions(view, viewsToPartitions(List(view))) ).headOption.getOrElse(null)

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
        true)
      conf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL,
        serverKerberosPrincipal)
      //conf.setIntVar(HiveConf.ConfVars.METASTORE_BATCH_RETRIEVE_MAX, 50000)
      //conf.setIntVar(HiveConf.ConfVars.METASTORESERVERMAXMESSAGESIZE, 1000*1024*1024)
    }
    val metastoreClient = new HiveMetaStoreClient(conf)
    new SchemaManager(metastoreClient, connection)
  }

  def apply(metastoreClient: IMetaStoreClient, connection: Connection) = {
    new SchemaManager(metastoreClient, connection)
  }
}