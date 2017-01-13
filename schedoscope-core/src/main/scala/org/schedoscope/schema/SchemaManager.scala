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
package org.schedoscope.schema

import java.security.PrivilegedAction
import java.sql.{Connection, DriverManager}

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.{AlreadyExistsException, Partition}
import org.apache.hadoop.hive.metastore.{HiveMetaStoreClient, IMetaStoreClient}
import org.apache.thrift.TException
import org.joda.time.DateTime
import org.schedoscope.Schedoscope.settings
import org.schedoscope.dsl.View
import org.schedoscope.dsl.transformations.Checksum
import org.schedoscope.schema.ddl.HiveQl
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.{asScalaBuffer, mapAsScalaMap, mutableMapAsJavaMap, seqAsJavaList}
import scala.collection.mutable.HashMap

/**
  * Exception class for wrapping retryable exceptions raised by the schema manager.
  * The schema-related actors (SchemaManager/PartitionCreator/MetadataLogger) will
  * restart when facing these exceptions.
  */
case class RetryableSchemaManagerException(message: String = null, cause: Throwable = null) extends RuntimeException(message, cause)

/**
  * Exception class for wrapping fatal exceptions raised by the schema manager with no prospect of a successful retry.
  * The schema-related actors (SchemaManager/PartitionCreator/MetadataLogger) will
  * restart when facing these exceptions.
  */
case class FatalSchemaManagerException(message: String = null, cause: Throwable = null) extends RuntimeException(message, cause)

/**
  * Interface to the Hive metastore. Used by partition creator actor and metadata logger actor.
  */
class SchemaManager(val metastoreClient: IMetaStoreClient, val connection: Connection) {
  private val existingSchemas = collection.mutable.Set[String]()

  private val log = LoggerFactory.getLogger(classOf[SchemaManager])

  /**
    * Create a partition for a view in the metastore (if it does not exist already) and return it.
    */
  def createPartition(view: View): Partition = {
    val partition = viewsToPartitions(List(view)).values.head
    //register in metastore
    if (view.isPartitioned()) {
      try {
        metastoreClient.add_partitions(List(partition), false, false)
      } catch {
        case _: Throwable => // Accept exceptions
      }
    }
    partition
  }

  private def viewsToPartitions(views: List[View]): Map[String, Partition] = {
    if (views.size == 0) {
      return Map()
    }

    val tablePrototype = views.head
    val table = metastoreClient.getTable(tablePrototype.dbName, tablePrototype.n)

    views.map { v =>
      val now = new DateTime().getMillis.toInt
      val sd = table.getSd().deepCopy()
      sd.setLocation(v.fullPath)

      (v.partitionSpec.replaceFirst("/", "") -> new Partition(v.partitionValues(), v.dbName, v.n, now, now, sd, HashMap[String, String]()))
    }.toMap
  }

  /**
    * Drop all databases in metastore, dropping tables, and deleting data within
    */
  def wipeMetastore: Unit = {

    val dbNames = try {
      metastoreClient.getAllDatabases
    } catch {
      case t: Throwable => {
        log.warn("Schema Manager unable to read databases to wipe.", t)
        return
      }
    }

    dbNames.foreach(
      db => try {
        metastoreClient.dropDatabase(db, true, true, true)
      } catch {
        case t: Throwable => {
          log.warn(s"Schema Manager failed to wipe database ${db}. Continuing.", t)
        }
      }
    )
  }

  /**
    * Execute create database and table for view, dropping the table should it exist already.
    *
    * It throws a RetryableSchemaManagerException in case of any problem, encapsulating the original exception.
    */
  def dropAndCreateTableSchema(view: View): Unit = try {
    val ddl = HiveQl.ddl(view)
    val stmt = connection.createStatement()

    try {
      stmt.execute(s"CREATE DATABASE IF NOT EXISTS ${view.dbName}")
    } catch {
      case t: Throwable =>
        log.warn(s"Could not create database ${view.dbName}. Maybe you simply do not have permission to execute CREATE TABLE statements. In this case, get an admin to create the database for you before running schedoscope.", t)
    }

    stmt.execute(s"DROP TABLE IF EXISTS ${view.dbName}.${view.n}")

    log.info(s"Creating table:\n${ddl}")

    stmt.execute(ddl)

    stmt.close()

    setTableProperty(view.dbName, view.n, Checksum.SchemaChecksum.checksumProperty, HiveQl.ddlChecksum(view))
  } catch {
    case t: Throwable => {
      log.error(s"Schema Manager failed to create table ${view.dbName}.${view.n}.")

      throw RetryableSchemaManagerException(s"Schema manager failed to create table ${view.dbName}.${view.n}", t)
    }
  }

  /**
    * Checks whether a database and table exists for the given view, as well as whether is has the correct form.
    *
    * Throws a RetryableSchemaManagerException in case of any problem, encapsulating the original exception.
    */
  def schemaExists(view: View): Boolean = try {
    val d = HiveQl.ddlChecksum(view)

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
      if (!props.containsKey(Checksum.SchemaChecksum.checksumProperty)) {
        log.info(s"Table for view exists ${view.dbName}.${view.n} but no checksum property defined")
        false
      } else if (d == props.get(Checksum.SchemaChecksum.checksumProperty).toString()) {
        log.info(s"Table for view exists ${view.dbName}.${view.n} and checksum ${d} matches")
        existingSchemas += d
        true
      } else {
        log.info(s"Table for view exists ${view.dbName}.${view.n} but checksum ${d} does not match")
        false
      }
    }
  } catch {
    case t: Throwable => {
      log.error(s"Schema Manager failed to check whether schema for view ${view.dbName}.${view.n} exists")
      throw RetryableSchemaManagerException(s"Schema Manager failed to check whether schema for view ${view.dbName}.${view.n} exists", t)
    }
  }

  /**
    * Store the transformation version checksum for the given view with the corresponding table partition in
    * the metastore.
    *
    * Throws a RetryableSchemaManagerException in case of a protiocol / connection problem with the metastore; a
    * FatalSchemaManagerException in case of any other issue.
    *
    */
  def setTransformationVersion(view: View) = try {
    if (view.isPartitioned()) {
      setPartitionProperty(view.dbName, view.n, view.partitionSpec, Checksum.TransformationChecksum.checksumProperty, view.transformation().checksum)
    } else {
      setTableProperty(view.dbName, view.n, Checksum.TransformationChecksum.checksumProperty, view.transformation().checksum)
    }
  } catch {
    case te: TException => {
      log.error(s"Schema Manager facing potentially recoverable Thrift protocol exception while setting transformation version in Metastore.", te)
      throw RetryableSchemaManagerException(s"Schema Manager facing potentially recoverable Thrift protocol exception while setting transformation version in Metastore.", te)
    }

    case t: Throwable => {
      log.error(s"Schema Manager facing unrecoverable Thrift protocol exception while setting transformation version in Metastore.", t)
      throw FatalSchemaManagerException(s"Schema Manager facing unrecoverable Thrift protocol exception while setting transformation version in Metastore.", t)
    }
  }

  private def setTableProperty(dbName: String, tableName: String, key: String, value: String): Unit = {
    val table = metastoreClient.getTable(dbName, tableName)
    table.putToParameters(key, value)
    metastoreClient.alter_table(dbName, tableName, table)
  }

  private def setPartitionProperty(dbName: String, tableName: String, part: String, key: String, value: String): Unit = {
    val partition = metastoreClient.getPartition(dbName, tableName, part)
    partition.putToParameters(key, value)
    metastoreClient.alter_partition(dbName, tableName, partition)
  }

  /**
    * Store the given transformation timestamp for the given view with the corresponding table partition in
    * the metastore.
    *
    * Throws a RetryableSchemaManagerException in case of a protiocol / connection problem with the metastore; a
    * FatalSchemaManagerException in case of any other issue.
    *
    */
  def setTransformationTimestamp(view: View, timestamp: Long) = try {
    if (view.isPartitioned()) {
      setPartitionProperty(view.dbName, view.n, view.partitionSpec, Checksum.TransformationChecksum.timestampProperty, timestamp.toString)
    } else {
      setTableProperty(view.dbName, view.n, Checksum.TransformationChecksum.timestampProperty, timestamp.toString)
    }
  } catch {
    case te: TException => {
      log.error(s"Schema Manager facing potentially recoverable Thrift protocol exception while setting transformation timestamp in Metastore.", te)
      throw RetryableSchemaManagerException(s"Schema Manager facing potentially recoverable Thrift protocol exception while setting transformation timestamp in Metastore.", te)
    }

    case t: Throwable => {
      log.error(s"Schema Manager facing unrecoverable Thrift protocol exception while setting transformation timestamp in Metastore.", t)
      throw FatalSchemaManagerException(s"Schema Manager facing unrecoverable Thrift protocol exception while setting transformation timestamp in Metastore.", t)
    }
  }

  /**
    * Retrieve transformation metadata for the given views relating to a single (!) table from the metastore. Grouped by view a tuple with
    * the transformation version checksum and timestamp of last transformation is returned.
    *
    * Throws a RetryableSchemaManagerException in case of a protocol / connection problem with the metastore; a
    * FatalSchemaManagerException in case of any other issue.
    *
    */
  def getTransformationMetadata(views: List[View]): Map[View, (String, Long)] = try {
    val tablePrototype = views.head

    log.info(s"Reading partition names for view: ${tablePrototype.module}.${tablePrototype.n}")

    if (!tablePrototype.isPartitioned) {
      log.info(s"View ${tablePrototype.module}.${tablePrototype.n} is not partitioned, returning metadata from table properties")
      return getExistingTransformationMetadata(tablePrototype, Map[String, Partition]())
    }

    val existingPartitionNames = metastoreClient.listPartitionNames(tablePrototype.dbName, tablePrototype.n, -1).map("/" + _).toSet

    val existingPartitions = viewsToPartitions(views.filter(v => existingPartitionNames.contains(v.partitionSpec)))

    val nonExistingPartitions = viewsToPartitions(views.filter(v => !existingPartitionNames.contains(v.partitionSpec)))

    log.info(s"Table for view ${tablePrototype.module}.${tablePrototype.n} requires partitions: ${existingPartitions.size} existing / ${nonExistingPartitions.size} not yet existing")

    log.info(s"Retrieving existing transformation metadata for view ${tablePrototype.module}.${tablePrototype.n} from partitions.")

    val existingMetadata = existingPartitions.grouped(settings.metastoreReadBatchSize)
      .map { ep => {
        log.info(s"Reading ${ep.size} partition metadata for view ${tablePrototype.module}.${tablePrototype.n}")
        getExistingTransformationMetadata(tablePrototype, ep)
      }
      }.reduceOption(_ ++ _).getOrElse(Map())

    val createdMetadata = nonExistingPartitions.grouped(settings.metastoreWriteBatchSize)
      .map(nep => {
        log.info(s"Creating ${nep.size} partitions for view ${tablePrototype.module}.${tablePrototype.n}")
        createNonExistingPartitions(tablePrototype, nep.values.toList)
      }).reduceOption(_ ++ _).getOrElse(Map())

    existingMetadata ++ createdMetadata

  } catch {
    case te: TException => {
      log.error(s"Schema Manager facing potentially recoverable Thrift protocol exception while retrieving transformation metadata from Metastore.", te)
      throw RetryableSchemaManagerException(s"Schema Manager facing Thrift protocol exception while retrieving transformation metadata from Metastore.", te)
    }

    case t: Throwable => {
      log.error(s"Schema Manager facing unrecoverable exception while retrieving transformation metadata from Metastore.", t)
      throw FatalSchemaManagerException(s"Schema Manager facing unrecoverable exception while retrieving transformation metadata from Metastore.", t)
    }
  }

  private def createNonExistingPartitions(tablePrototype: View, partitions: List[Partition], retry: Int = 3): Map[View, (String, Long)] = try {
    if (partitions.isEmpty || !tablePrototype.isPartitioned()) {
      Map()
    } else {
      metastoreClient.add_partitions(partitions, false, false)
      partitions.map(p => (partitionToView(tablePrototype, p) -> (Checksum.defaultDigest, 0.toLong))).toMap
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

  private def getExistingTransformationMetadata(tablePrototype: View, partitions: Map[String, Partition]): Map[View, (String, Long)] = {
    if (tablePrototype.isPartitioned) {
      val existingPartitions = metastoreClient.getPartitionsByNames(tablePrototype.dbName, tablePrototype.n, partitions.keys.toList)
      existingPartitions.map { p => (partitionToView(tablePrototype, p), (p.getParameters.getOrElse(Checksum.TransformationChecksum.checksumProperty, Checksum.defaultDigest), p.getParameters.getOrElse(Checksum.TransformationChecksum.timestampProperty, "0").toLong)) }.toMap
    } else {
      val tableMetadata = metastoreClient.getTable(tablePrototype.dbName, tablePrototype.n).getParameters
      Map((tablePrototype,
        (tableMetadata.getOrElse(Checksum.TransformationChecksum.checksumProperty, Checksum.defaultDigest),
          tableMetadata.getOrElse(Checksum.TransformationChecksum.timestampProperty, "0").toLong)))
    }

  }

  private def partitionToView(tablePrototype: View, p: Partition) = {
    val viewUrl = s"${tablePrototype.urlPathPrefix}/${p.getValues.mkString("/")}"
    View.viewsFromUrl(settings.env,
      viewUrl,
      settings.viewAugmentor).head
  }

}

object SchemaManager {

  def apply(jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String): SchemaManager = try {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val connection =
      settings.userGroupInformation.doAs(new PrivilegedAction[Connection]() {
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

    SchemaManager(metastoreClient, connection)
  } catch {
    case te: TException => {
      throw RetryableSchemaManagerException(s"Schema Manager initialization facing Thrift protocol exception.", te)
    }

    case t: Throwable => {
      throw FatalSchemaManagerException(s"Schema Manager facing unrecoverable exception while initializing.", t)
    }
  }

  def apply(metastoreClient: IMetaStoreClient, connection: Connection) =
    new SchemaManager(metastoreClient, connection)


}
