/**
 * Copyright 2016 Otto (GmbH & Co KG)
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

package org.schedoscope.dsl.transformations

import org.apache.hadoop.mapreduce.Job
import org.schedoscope.Settings
import org.schedoscope.dsl.{ Field, View }
import org.schedoscope.export.jdbc.JdbcExportJob
import org.schedoscope.export.jdbc.exception.{ RetryException, UnrecoverableException }
import org.schedoscope.export.redis.RedisExportJob
import org.schedoscope.scheduler.driver.{ Driver, DriverRunFailed, DriverRunState, DriverRunSucceeded, RetryableDriverException }
import org.schedoscope.Schedoscope
import org.schedoscope.export.kafka.KafkaExportJob
import org.schedoscope.export.kafka.options.ProducerType
import org.schedoscope.export.kafka.options.CleanupPolicy
import org.schedoscope.export.kafka.options.CompressionCodec
import org.schedoscope.export.kafka.options.OutputEncoding

/**
 * A helper class to with constructors for exportTo() MR jobs.
 */
object Export {

  /**
   * This function configures the JDBC export job and returns a MapreduceTransformation.
   *
   * @param v The view to export
   * @param jdbcConnection A JDBC connection string
   * @param dbUser The database user
   * @param dbPass the database password
   * @param distributionKey The distribution key (only relevant for exasol)
   * @param anonFields A list of fields to anonymize
   * @param exportSalt an optional salt when anonymizing fields
   * @param storageEngine The underlying storage engine (only relevant for MySQL)
   * @param numReducers The number of reducers, defines concurrency
   * @param commitSize The size of batches for JDBC inserts
   * @param isKerberized Is the cluster kerberized?
   * @param kerberosPrincipal The kerberos principal to use
   * @param metastoreUri The thrift URI to the metastore
   */
  def Jdbc(
    v: View,
    jdbcConnection: String,
    dbUser: String = null,
    dbPass: String = null,
    distributionKey: Field[_] = null,
    anonFields: Seq[Field[_]] = null,
    exportSalt: String = Schedoscope.settings.exportSalt,
    storageEngine: String = Schedoscope.settings.jdbcStorageEngine,
    numReducers: Int = Schedoscope.settings.jdbcExportNumReducers,
    commitSize: Int = Schedoscope.settings.jdbcExportBatchSize,
    isKerberized: Boolean = !Schedoscope.settings.kerberosPrincipal.isEmpty(),
    kerberosPrincipal: String = Schedoscope.settings.kerberosPrincipal,
    metastoreUri: String = Schedoscope.settings.metastoreUri) = {

    val t = MapreduceTransformation(
      v,
      (conf) => {

        val filter = v.partitionParameters
          .map { (p => s"${p.n} = '${p.v.get}'") }
          .mkString(" and ")

        val distributionField = if (distributionKey != null) distributionKey.n else null

        val anonFieldNames = if (anonFields == null) new Array[String](0) else anonFields.map { _.n } .toArray

        new JdbcExportJob().configure(
          conf.get("schedoscope.export.isKerberized").get.asInstanceOf[Boolean],
          conf.get("schedoscope.export.metastoreUri").get.asInstanceOf[String],
          conf.get("schedoscope.export.kerberosPrincipal").get.asInstanceOf[String],
          conf.get("schedoscope.export.jdbcConnection").get.asInstanceOf[String],
          conf.get("schedoscope.export.dbUser").getOrElse(null).asInstanceOf[String],
          conf.get("schedoscope.export.dbPass").getOrElse(null).asInstanceOf[String],
          v.dbName,
          v.n,
          filter,
          conf.get("schedoscope.export.storageEngine").get.asInstanceOf[String],
          distributionField,
          conf.get("schedoscope.export.numReducers").get.asInstanceOf[Int],
          conf.get("schedoscope.export.commitSize").get.asInstanceOf[Int],
          anonFieldNames,
          conf.get("schedoscope.export.salt").get.asInstanceOf[String])

      },
      jdbcPostCommit)

    t.directoriesToDelete = List()
    t.configureWith(
      Map(
        "schedoscope.export.jdbcConnection" -> jdbcConnection,
        "schedoscope.export.dbUser" -> dbUser,
        "schedoscope.export.dbPass" -> dbPass,
        "schedoscope.export.storageEngine" -> storageEngine,
        "schedoscope.export.numReducers" -> numReducers,
        "schedoscope.export.commitSize" -> commitSize,
        "schedoscope.export.salt" -> exportSalt,
        "schedoscope.export.isKerberized" -> isKerberized,
        "schedoscope.export.kerberosPrincipal" -> kerberosPrincipal,
        "schedoscope.export.metastoreUri" -> metastoreUri))

  }

  /**
   * This function runs the post commit action and finalizes the database tables.
   *
   * @param job The MR job object
   * @param driver The schedoscope driver
   * @param runState The job's runstate
   */
  def jdbcPostCommit(
    job: Job,
    driver: Driver[MapreduceTransformation],
    runState: DriverRunState[MapreduceTransformation]): DriverRunState[MapreduceTransformation] = {

    val jobConfigurer = new JdbcExportJob()

    try {

      jobConfigurer.postCommit(runState.isInstanceOf[DriverRunSucceeded[MapreduceTransformation]], job.getConfiguration)
      runState

    } catch {
      case ex: RetryException         => throw new RetryableDriverException(ex.getMessage, ex)
      case ex: UnrecoverableException => DriverRunFailed(driver, ex.getMessage, ex)
    }
  }

  /**
   * This function configures the Redis export job and returns a MapreduceTransformation.
   *
   * @param v The view
   * @param redisHost The Redis hostname
   * @param key The field to use as the Redis key
   * @param value An optional field to export. If null, all fields are attached to the key as a map. If not null, only that field's value is attached to the key.
   * @param keyPrefix An optional key prefix
   * @param anonFields A list of fields to anonymize
   * @param exportSalt an optional salt when anonymizing fields
   * @param replace A flag indicating of existing keys should be replaced (or extended)
   * @param flush A flag indicating if the key space should be flushed before writing data
   * @param redisPort The Redis port (default 6379)
   * @param redisKeySpace The Redis key space (default 0)
   * @param numReducers The number of reducers, defines concurrency
   * @param pipeline A flag indicating that the Redis pipeline mode should be used for writing data
   * @param isKerberized Is the cluster kerberized?
   * @param kerberosPrincipal The kerberos principal to use
   * @param metastoreUri The thrift URI to the metastore
   */
  def Redis(
    v: View,
    redisHost: String,
    key: Field[_],
    value: Field[_] = null,
    keyPrefix: String = "",
    anonFields: Seq[Field[_]] = null,
    exportSalt: String = Schedoscope.settings.exportSalt,
    replace: Boolean = true,
    flush: Boolean = false,
    redisPort: Int = 6379,
    redisKeySpace: Int = 0,
    numReducers: Int = Schedoscope.settings.redisExportNumReducers,
    pipeline: Boolean = Schedoscope.settings.redisExportUsesPipelineMode,
    isKerberized: Boolean = !Schedoscope.settings.kerberosPrincipal.isEmpty(),
    kerberosPrincipal: String = Schedoscope.settings.kerberosPrincipal,
    metastoreUri: String = Schedoscope.settings.metastoreUri) = {

    val t = MapreduceTransformation(
      v,
      (conf) => {

        val filter = v.partitionParameters
          .map { (p => s"${p.n} = '${p.v.get}'") }
          .mkString(" and ")

        val valueFieldName = if (value != null) value.n else null

        val anonFieldNames = if (anonFields == null) new Array[String](0) else anonFields.map { _.n } .toArray

        new RedisExportJob().configure(
          conf.get("schedoscope.export.isKerberized").get.asInstanceOf[Boolean],
          conf.get("schedoscope.export.metastoreUri").get.asInstanceOf[String],
          conf.get("schedoscope.export.kerberosPrincipal").get.asInstanceOf[String],
          conf.get("schedoscope.export.redisHost").get.asInstanceOf[String],
          conf.get("schedoscope.export.redisPort").get.asInstanceOf[Int],
          conf.get("schedoscope.export.redisKeySpace").get.asInstanceOf[Int],
          v.dbName,
          v.n,
          filter,
          key.n,
          valueFieldName,
          keyPrefix,
          conf.get("schedoscope.export.numReducers").get.asInstanceOf[Int],
          replace,
          conf.get("schedoscope.export.pipeline").get.asInstanceOf[Boolean],
          flush,
          anonFieldNames,
          conf.get("schedoscope.export.salt").get.asInstanceOf[String])

      })

    t.directoriesToDelete = List()
    t.configureWith(
      Map(
        "schedoscope.export.redisHost" -> redisHost,
        "schedoscope.export.redisPort" -> redisPort,
        "schedoscope.export.redisKeySpace" -> redisKeySpace,
        "schedoscope.export.numReducers" -> numReducers,
        "schedoscope.export.pipeline" -> pipeline,
        "schedoscope.export.salt" -> exportSalt,
        "schedoscope.export.isKerberized" -> isKerberized,
        "schedoscope.export.kerberosPrincipal" -> kerberosPrincipal,
        "schedoscope.export.metastoreUri" -> metastoreUri))

  }

  /**
   * This function creates a Kafka topic export MapreduceTransformation.
   *
   * @param v The view to export
   * @param key the field to serve as the topic's key
   * @param kafkaHosts String list of Kafka hosts to communicate with
   * @param zookeeperHosts String list of zookeeper hosts
   * @param replicationFactor The replication factor, defaults to 1
   * @param numPartitions The number of partitions in the topic. Defaults to 3
   * @param anonFields A list of fields to anonymize
   * @param exportSalt an optional salt when anonymizing fields
   * @param producerType The type of producer to use, defaults to synchronous
   * @param cleanupPolicy Default cleanup policy is delete
   * @param compressionCodes Default compression codec is gzip
   * @param encoding Defines, whether data is to be serialized as strings (one line JSONs) or Avro
   * @param numReducers number of reducers to use (i.e., the parallelism)
   * @param isKerberized Is the cluster kerberized?
   * @param kerberosPrincipal The kerberos principal to use
   * @param metastoreUri The thrift URI to the metastore
   *
   */
  def Kafka(
    v: View,
    key: Field[_],
    kafkaHosts: String,
    zookeeperHosts: String,
    replicationFactor: Int = 1,
    numPartitons: Int = 3,
    anonFields: Seq[Field[_]] = null,
    exportSalt: String = Schedoscope.settings.exportSalt,
    producerType: ProducerType = ProducerType.sync,
    cleanupPolicy: CleanupPolicy = CleanupPolicy.delete,
    compressionCodec: CompressionCodec = CompressionCodec.gzip,
    encoding: OutputEncoding = OutputEncoding.string,
    numReducers: Int = Schedoscope.settings.kafkaExportNumReducers,
    isKerberized: Boolean = !Schedoscope.settings.kerberosPrincipal.isEmpty(),
    kerberosPrincipal: String = Schedoscope.settings.kerberosPrincipal,
    metastoreUri: String = Schedoscope.settings.metastoreUri) = {

    val t = MapreduceTransformation(
      v,
      (conf) => {

        val filter = v.partitionParameters
          .map { (p => s"${p.n} = '${p.v.get}'") }
          .mkString(" and ")

        val anonFieldNames = if (anonFields == null) new Array[String](0) else anonFields.map { _.n } .toArray

        new KafkaExportJob().configure(
          conf.get("schedoscope.export.isKerberized").get.asInstanceOf[Boolean],
          conf.get("schedoscope.export.metastoreUri").get.asInstanceOf[String],
          conf.get("schedoscope.export.kerberosPrincipal").get.asInstanceOf[String],
          v.dbName,
          v.n,
          filter,
          key.n,
          conf.get("schedoscope.export.kafkaHosts").get.asInstanceOf[String],
          conf.get("schedoscope.export.zookeeperHosts").get.asInstanceOf[String],
          producerType,
          cleanupPolicy,
          conf.get("schedoscope.export.numPartitions").get.asInstanceOf[Int],
          conf.get("schedoscope.export.replicationFactor").get.asInstanceOf[Int],
          conf.get("schedoscope.export.numReducers").get.asInstanceOf[Int],
          compressionCodec,
          encoding,
          anonFieldNames,
          conf.get("schedoscope.export.salt").get.asInstanceOf[String])
      })

    t.directoriesToDelete = List()
    t.configureWith(
      Map(
        "schedoscope.export.kafkaHosts" -> kafkaHosts,
        "schedoscope.export.zookeeperHosts" -> zookeeperHosts,
        "schedoscope.export.numPartitions" -> numPartitons,
        "schedoscope.export.replicationFactor" -> replicationFactor,
        "schedoscope.export.numReducers" -> numReducers,
        "schedoscope.export.salt" -> exportSalt,
        "schedoscope.export.isKerberized" -> isKerberized,
        "schedoscope.export.kerberosPrincipal" -> kerberosPrincipal,
        "schedoscope.export.metastoreUri" -> metastoreUri))
  }
}
