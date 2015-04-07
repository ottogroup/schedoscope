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
import org.slf4j.LoggerFactory
import org.apache.tools.ant.taskdefs.Sleep

class SchemaManager(val metastoreClient: IMetaStoreClient, val connection: Connection) {
  val md5 = MessageDigest.getInstance("MD5")
  val existingSchemas = collection.mutable.Set[String]()

  val log = LoggerFactory.getLogger(classOf[SchemaManager])

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

  def dropAndCreateTableSchema(view: View): Unit = {
    val ddl = HiveQl.ddl(view)
    val stmt = connection.createStatement()
    stmt.execute(s"CREATE DATABASE IF NOT EXISTS ${view.dbName}")
    stmt.execute(s"DROP TABLE IF EXISTS ${view.dbName}.${view.n}")

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

  def partitionToView(tablePrototype: View, p: Partition) = {
    val viewUrl = s"${tablePrototype.urlPathPrefix}/${p.getValues.mkString("/")}"
    View.viewsFromUrl(Settings().env,
      viewUrl,
      Settings().viewAugmentor).head
  }

  def viewsToPartitions(views: List[View]): Map[String, Partition] = {
    if (views.size == 0) {
      return Map()
    }

    val tablePrototype = views.head
    val sd = metastoreClient.getTable(tablePrototype.dbName, tablePrototype.n).getSd

    views.map { v =>
      val now = new DateTime().getMillis.toInt
      sd.setLocation(v.fullPath)

      (v.partitionSpec.replaceFirst("/", "") -> new Partition(v.partitionValues, v.dbName, v.n, now, now, sd, HashMap[String, String]()))
    }.toMap
  }

  def createNonExistingPartitions(tablePrototype: View, partitions: List[Partition], retry: Int = 3): Map[View, (String, Long)] = try {
    if (partitions.isEmpty || !tablePrototype.isPartitioned()) {
      Map()
    } else {
      metastoreClient.add_partitions(partitions, false, false)
      partitions.map(p => (partitionToView(tablePrototype, p) -> (Version.default, 0.toLong))).toMap
    }
  } catch {
    case t: Throwable => if (retry > 0) {
      log.info(s"Caught exception ${t}, retrying")
      Thread.sleep(5000)
      createNonExistingPartitions(tablePrototype, partitions, retry - 1)
    } else 
      throw t
  }

  def getExistingTransformationMetadata(tablePrototype: View, partitions: Map[String, Partition]): Map[View, (String, Long)] = {
    if (tablePrototype.isPartitioned) {
      val existingPartitions = metastoreClient.getPartitionsByNames(tablePrototype.dbName, tablePrototype.n, partitions.keys.toList)
      existingPartitions.map { p => (partitionToView(tablePrototype, p), (p.getParameters.getOrElse(Version.TransformationVersion.checksumProperty, Version.default), p.getParameters.getOrElse(Version.TransformationVersion.timestampProperty, "0").toLong)) }.toMap
    } else {
      val tableMetadata = metastoreClient.getTable(tablePrototype.dbName, tablePrototype.n).getParameters
      Map((tablePrototype,
        (tableMetadata.getOrElse(Version.TransformationVersion.checksumProperty, Version.default),
          tableMetadata.getOrElse(Version.TransformationVersion.timestampProperty, "0").toLong)))
    }
  }

  def createPartition(view: View): Partition = {
    val partition = viewsToPartitions(List(view)).values.head
    try {
      createNonExistingPartitions(view, List(partition))
    } catch {
      case are: AlreadyExistsException => // This is allowed here
      case t: Throwable => t
    }
    partition
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

  def getTransformationMetadata(views: List[View]): Map[View, (String, Long)] = {
    val tablePrototype = views.head

    log.info(s"Reading partition names for view: ${tablePrototype.module}.${tablePrototype.n}")

    if (!schemaExists(tablePrototype)) {
      log.info(s"Table for view ${tablePrototype.module}.${tablePrototype.n} does not yet exist, creating")
      dropAndCreateTableSchema(tablePrototype)
    }

    if (!tablePrototype.isPartitioned) {
      log.info(s"View ${tablePrototype.module}.${tablePrototype.n} is not partitioned, returning metadata from table properties")
      return getExistingTransformationMetadata(tablePrototype, Map[String, Partition]())
    }

    val existingPartitionNames = metastoreClient.listPartitionNames(tablePrototype.dbName, tablePrototype.n, -1).map("/" + _).toSet

    val existingPartitions = viewsToPartitions(views.filter(v => existingPartitionNames.contains(v.partitionSpec)))
    val nonExistingPartitions = viewsToPartitions(views.filter(v => !existingPartitionNames.contains(v.partitionSpec)))

    log.info(s"Table for view ${tablePrototype.module}.${tablePrototype.n} requires partitions: ${existingPartitions.size} existing / ${nonExistingPartitions.size} not yet existing")

    log.info(s"Retrieving existing transformation metadata for view ${tablePrototype.module}.${tablePrototype.n} from partitions.")

    val existingMetadata = existingPartitions.grouped(Settings().metastoreReadBatchSize)
      .map { ep =>
        {
          log.info(s"Reading ${ep.size} partition metadata for view ${tablePrototype.module}.${tablePrototype.n}")
          getExistingTransformationMetadata(tablePrototype, ep)
        }
      }.reduceOption(_ ++ _).getOrElse(Map())

    val createdMetadata = nonExistingPartitions.grouped(Settings().metastoreWriteBatchSize)
      .map(nep => {
        log.info(s"Creating ${nep.size} partitions for view ${tablePrototype.module}.${tablePrototype.n}")
        createNonExistingPartitions(tablePrototype, nep.values.toList)
      }).reduceOption(_ ++ _).getOrElse(Map())

    existingMetadata ++ createdMetadata
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