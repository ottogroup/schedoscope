package org.schedoscope.schema

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
import org.schedoscope.dsl.Version
import com.ottogroup.bi.soda.dsl.View
import org.slf4j.LoggerFactory
import org.apache.tools.ant.taskdefs.Sleep
import com.ottogroup.bi.soda.dsl.ExternalTransformation
import org.hamcrest.core.IsInstanceOf

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

  def setTransformationVersion(view: View) = {
    if (view.isExternal()) {
      setTableProperty(view.dbName, view.n, Version.TransformationVersion.checksumProperty, view.transformation().versionDigest)

    } else if (view.isPartitioned()) {
      setPartitionProperty(view.dbName, view.n, view.partitionSpec, Version.TransformationVersion.checksumProperty, view.transformation().versionDigest)
    } else {
      setTableProperty(view.dbName, view.n, Version.TransformationVersion.checksumProperty, view.transformation().versionDigest)
    }
  }

  def setTransformationTimestamp(view: View, timestamp: Long) = {
    if (view.isExternal()) {
      setTableProperty(view.dbName, view.n, Version.TransformationVersion.timestampProperty, timestamp.toString)

    } else if (view.isPartitioned()) {
      setPartitionProperty(view.dbName, view.n, view.partitionSpec, Version.TransformationVersion.timestampProperty, timestamp.toString)
    } else {
      setTableProperty(view.dbName, view.n, Version.TransformationVersion.timestampProperty, timestamp.toString)
    }
  }

  def dropAndCreateTableSchema(view: View): Unit = {
    val ddl = HiveQl.ddl(view)

    val stmt = connection.createStatement()

    try {
      stmt.execute(s"CREATE DATABASE IF NOT EXISTS ${view.dbName}")
    } catch {
      case _: Throwable =>
    }

    try {
      stmt.execute(s"DROP TABLE IF EXISTS ${view.dbName}.${view.n}")
    } catch {
      case _: Throwable =>
    }

    log.info(s"Creating table:\n${ddl}")

    stmt.execute(ddl)

    stmt.close()

    setTableProperty(view.dbName, view.n, Version.SchemaVersion.checksumProperty, Version.digest(ddl))
  }

  def schemaExists(view: View): Boolean = {
    val d = Version.digest(HiveQl.ddl(view))

    log.info(s"Checking whether table exists: view ${view.dbName}.${view.n} -- Checksum: ${d}")

    if (existingSchemas.contains(d)) {
      log.info(s"Table for view ${view.dbName}.${view.n} found in schema cache")
      return true
    }

    if (!metastoreClient.tableExists(view.dbName, view.n)) {
      log.info(s"Metastore claims table for view ${view.dbName}.${view.n} does not exist")
      false
    } else {
      val table = metastoreClient.getTable(view.dbName, view.n)

      val props = table.getParameters()
      if (!props.containsKey(Version.SchemaVersion.checksumProperty)) {
        log.info(s"Table for view exists ${view.dbName}.${view.n} but no checksum property defined")
        false
      } else if (d == props.get(Version.SchemaVersion.checksumProperty).toString()) {
        log.info(s"Table for view exists ${view.dbName}.${view.n} and checksum ${d} matches")
        existingSchemas += d
        true
      } else {
        log.info(s"Table for view exists ${view.dbName}.${view.n} but checksum ${d} does not match")
        false
      }
    }
  }

  def createNonExistingPartitions(tablePrototype: View, partitions: List[Partition], retry: Int = 3): Map[View, (String, Long)] = try {
    if (partitions.isEmpty || !tablePrototype.isPartitioned() || tablePrototype.transformation().isInstanceOf[ExternalTransformation]) {
      Map()
    } else {
      metastoreClient.add_partitions(partitions, false, false)
      partitions.map(p => (partitionToView(tablePrototype, p) -> (Version.default, 0.toLong))).toMap
    }
  } catch {
    case are: AlreadyExistsException => throw are

    case t: Throwable => if (retry > 0) {
      log.info(s"Caught exception ${t}, retrying")
      Thread.sleep(5000)
      createNonExistingPartitions(tablePrototype, partitions, retry - 1)
    } else {
      throw t
    }
  }

  def createPartition(view: View): Partition = {
    val partition = viewsToPartitions(List(view)).values.head
    try {
      createNonExistingPartitions(view, List(partition))
    } catch {
      case _: Throwable => // Accept exceptions
    }
    partition
  }

  def getExistingTransformationMetadata(tablePrototype: View, partitions: Map[String, Partition]): Map[View, (String, Long)] = {
    if (tablePrototype.isExternal) {
      val dbMetadata = metastoreClient.getDatabase(tablePrototype.dbName).getParameters
      Map((tablePrototype,
        (dbMetadata.getOrElse(Version.TransformationVersion.checksumProperty, Version.default),
          dbMetadata.getOrElse(Version.TransformationVersion.timestampProperty, "0").toLong)))
    } else if (tablePrototype.isPartitioned) {
      val existingPartitions = metastoreClient.getPartitionsByNames(tablePrototype.dbName, tablePrototype.n, partitions.keys.toList)
      existingPartitions.map { p => (partitionToView(tablePrototype, p), (p.getParameters.getOrElse(Version.TransformationVersion.checksumProperty, Version.default), p.getParameters.getOrElse(Version.TransformationVersion.timestampProperty, "0").toLong)) }.toMap
    } else {
      val tableMetadata = metastoreClient.getTable(tablePrototype.dbName, tablePrototype.n).getParameters
      Map((tablePrototype,
        (tableMetadata.getOrElse(Version.TransformationVersion.checksumProperty, Version.default),
          tableMetadata.getOrElse(Version.TransformationVersion.timestampProperty, "0").toLong)))
    }

  }

  def getTransformationMetadata(views: List[View]): Map[View, (String, Long)] = {
    val tablePrototype = views.head

    log.info(s"Reading partition names for view: ${tablePrototype.module}.${tablePrototype.n}")

    if (!tablePrototype.isPartitioned || tablePrototype.isExternal()) {
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
    val table = metastoreClient.getTable(tablePrototype.dbName, tablePrototype.n)

    views.map { v =>
      val now = new DateTime().getMillis.toInt
      val sd = table.getSd().deepCopy()
      sd.setLocation(v.fullPath)

      (v.partitionSpec.replaceFirst("/", "") -> new Partition(v.partitionValues, v.dbName, v.n, now, now, sd, HashMap[String, String]()))
    }.toMap
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
    }
    val metastoreClient = new HiveMetaStoreClient(conf)
    new SchemaManager(metastoreClient, connection)
  }

  def apply(metastoreClient: IMetaStoreClient, connection: Connection) = {
    new SchemaManager(metastoreClient, connection)
  }
}