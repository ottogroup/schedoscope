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
package org.schedoscope.conf

import java.util.concurrent.TimeUnit

import com.typesafe.config.Config

import scala.concurrent.duration.Duration

/**
  * The Settings class keeps all settings for Schedoscope. It is a singelton accessible through
  * the companion object's apply() method.
  *
  * Configuration is based on TypeSafe config. For an explanation of the different
  * configuration settings, please refer to src/main/resources/reference.conf
  *
  */
class BaseSettings(val config: Config) {

  /**
    * The configured Schedoscope environment
    */
  lazy val env = config.getString("schedoscope.app.environment")

  /**
    * The configured host of the Schedoscope web service (for calling via the SchedoscopeRestClient).
    */
  lazy val host = config.getString("schedoscope.webservice.host")

  /**
    * The configured post of the Schedoscope web service (for both web service and client)
    */
  lazy val port = config.getInt("schedoscope.webservice.port")

  /**
    * Configured directory for storing static web service resource data for Schedoscope web service
    */
  lazy val webResourcesDirectory = config.getString("schedoscope.webservice.resourceDirectory")

  /**
    * Configured actors for Schedoscope web service.
    */
  lazy val restApiConcurrency = config.getInt("schedoscope.webservice.concurrency")

  /**
    * Configured JDBC URL to the Hive server
    */
  lazy val jdbcUrl = config.getString("schedoscope.metastore.jdbcUrl")

  /**
    * Configured kerberos principal.
    */
  lazy val kerberosPrincipal = config.getString("schedoscope.kerberos.principal")

  /**
    * Configured Thrift metastore URI.
    */
  lazy val metastoreUri = config.getString("schedoscope.metastore.metastoreUri")

  /**
    * Configured path to Hive configuration
    */
  lazy val hiveConfDir = config.getString("schedoscope.metastore.hiveConfDir")

  /**
    * Configured view augmentor class for postprocessing of views after URL parsing
    */
  lazy val parsedViewAugmentorClass = config.getString("schedoscope.app.parsedViewAugmentorClass")

  /**
    * The subconfigurations of the configured transformation types.
    */
  lazy val availableTransformations = config.getObject("schedoscope.transformations")

  /**
    * The URI of the HDFS.
    */
  lazy val hdfs = config.getString("schedoscope.hadoop.hdfs")

  /**
    * Configuration trigger whether versioning transformation is enabled.
    */
  lazy val transformationVersioning = config.getBoolean("schedoscope.versioning.transformations")

  /**
    * The configured timeout for schema / metastore operations.
    */
  lazy val schemaTimeout = Duration.create(config.getDuration("schedoscope.scheduler.timeouts.schema", TimeUnit.SECONDS), TimeUnit.SECONDS)

  /**
    * Timeout for completion of scheduling commands
    */
  lazy val schedulingCommandTimeout = Duration.create(config.getDuration("schedoscope.scheduler.timeouts.schedulingCommand", TimeUnit.SECONDS), TimeUnit.SECONDS)

  /**
    * The configured timeout for Schedoscope web service calls.
    */
  lazy val webserviceTimeout =
    Duration.create(config.getDuration("schedoscope.scheduler.timeouts.schedulingCommand", TimeUnit.MILLISECONDS),
      TimeUnit.MILLISECONDS)

  /**
    * The configured number of retries before a view enters failed state.
    */
  lazy val retries = config.getInt("schedoscope.action.retry")

  /**
    * Flag for enabling usage of external dependencies
    */
  lazy val externalDependencies = config.getBoolean("schedoscope.external.enabled")

  /**
    * Flag for disabling checks for external dependencies
    */
  lazy val externalChecksDisabled = config.getBoolean("schedoscope.external.checks")


  /**
    * Number of parallel threads to access the metastore
    */
  lazy val metastoreConcurrency = config.getInt("schedoscope.metastore.concurrency")

  /**
    * Number of partitions to write in batch to the metatatore
    */
  lazy val metastoreWriteBatchSize = config.getInt("schedoscope.metastore.writeBatchSize")

  /**
    * Number of partitions to read in batch to the metatatore
    */
  lazy val metastoreReadBatchSize = config.getInt("schedoscope.metastore.readBatchSize")

  /**
    * A salt to use when anonymizing fields during export
    */
  lazy val exportSalt = config.getString("schedoscope.export.salt")

  /**
    * Number of reducers to use for JDBC export.
    */
  lazy val jdbcExportNumReducers = config.getInt("schedoscope.export.jdbc.numberOfReducers")

  /**
    * Size of insert batches for JDBC export.
    */
  lazy val jdbcExportBatchSize = config.getInt("schedoscope.export.jdbc.insertBatchSize")

  /**
    * Storage engine to use for JDBC export where relevant (MySQL so far).
    */
  lazy val jdbcStorageEngine = config.getString("schedoscope.export.jdbc.storageEngine")

  /**
    * Number of reducers to use for Redis export.
    */
  lazy val redisExportNumReducers = config.getInt("schedoscope.export.redis.numberOfReducers")

  /**
    * Use pipeline mode for writing to redis or not.
    */
  lazy val redisExportUsesPipelineMode = config.getBoolean("schedoscope.export.redis.usePipelineMode")

  /**
    * Size of insert batches for Redis export (only pipeline mode)
    */
  lazy val redisExportBatchSize = config.getInt("schedoscope.export.redis.insertBatchSize")

  /**
    * Number of reducers to use for Redis export.
    */
  lazy val kafkaExportNumReducers = config.getInt("schedoscope.export.kafka.numberOfReducers")

  /**
    * Number of reducers to use for (S)Ftp export.
    */
  lazy val ftpExportNumReducers = config.getInt("schedoscope.export.ftp.numberOfReducers")

  /**
    * Port of Metascope web service.
    */
  lazy val metascopePort = config.getInt("schedoscope.metascope.port")

  /**
    * Authentication method used for Metascope. Possible options: ['simple', 'ldap']
    */
  lazy val metascopeAuthMethod = config.getString("schedoscope.metascope.auth.authentication")

  /**
    * LDAP URL used for LDAP Authentication
    */
  lazy val metascopeLdapUrl = config.getString("schedoscope.metascope.auth.ldap.url")

  /**
    * LDAP manager distinguished name
    */
  lazy val metascopeLdapManagerDn = config.getString("schedoscope.metascope.auth.ldap.managerDn")

  /**
    * LDAP manager password
    */
  lazy val metascopeLdapManagerPw = config.getString("schedoscope.metascope.auth.ldap.managerPassword")

  /**
    * LDAP user distinguished name
    */
  lazy val metascopeLdapUserDn = config.getString("schedoscope.metascope.auth.ldap.userDnPattern")

  /**
    * LDAP group search base string
    */
  lazy val metascopeLdapGroupSearchBase = config.getString("schedoscope.metascope.auth.ldap.groupSearchBase")

  /**
    * Allowed LDAP groups (users) which can access Metascope
    */
  lazy val metascopeLdapAllowedGroups = config.getString("schedoscope.metascope.auth.ldap.allowedGroups")

  /**
    * User in the specified admin groups have Metascope admin permissions
    */
  lazy val metascopeLdapAdminGroups = config.getString("schedoscope.metascope.auth.ldap.adminGroups")

  /**
    * URL to metadata repository
    */
  lazy val metascopeRepositoryUrl = config.getString("schedoscope.metascope.repository.url")

  /**
    * User to access the metadata repository
    */
  lazy val metascopeRepositoryUser = config.getString("schedoscope.metascope.repository.user")

  /**
    * Password for the user used to access the metadata repository
    */
  lazy val metascopeRepositoryPw = config.getString("schedoscope.metascope.repository.password")

  /**
    * SQL Dialect for the specified database
    */
  lazy val metascopeRepositoryDialect = config.getString("schedoscope.metascope.repository.dialect")

  /**
    * URL to Solr instance
    */
  lazy val metascopeSolrUrl = config.getString("schedoscope.metascope.solr.url")

  /**
    * Location of the Metascope log file
    */
  lazy val metascopeLoggingFile = config.getString("schedoscope.metascope.logging.logfile")

  /**
    * Logging level of Metascope
    */
  lazy val metascopeLoggingLevel = config.getString("schedoscope.metascope.logging.loglevel")
}