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

/**
 * A helper class to configure the export MR jobs.
 */
object Export {

  /**
   * This function configures the JDBC export job and returns a MapreduceTransformation.
   *
   * @param v The view to export
   * @param dbConn A JDBC connection string
   * @param dbUser The database user
   * @param dbPass the database password
   * @param distributionKey The distribution key (only relevant for exasol)
   * @param storageEngine The underlying storage engine (only relevant for MySQL)
   * @param numReducer The number of reducers, defines concurrency
   * @param commitSize The size of batches for JDBC inserts
   */
  def jdbcExport(v: View, dbConn: String, dbUser: String, dbPass: String, distributionKey: Field[_] = null, storageEngine: String = "InnoDB", numReducer: Int = 10, commitSize: Int = 10000) =
    MapreduceTransformation(v, (config) => {

      val partitionParameters = v.partitionParameters
      val filter = partitionParameters.map { p => s"${p.n} = '${p.v.get}'" }
        .mkString(" and ")

      val distributionField = if (distributionKey != null) distributionKey.n else null

      val settings = Settings.apply()
      val secure = if (settings.kerberosPrincipal.isEmpty) false else true
      val jobConfigurer = new JdbcExportJob()

      jobConfigurer.configure(secure, settings.metastoreUri, settings.kerberosPrincipal, dbConn, dbUser, dbPass,
        v.dbName, v.tableName, filter, storageEngine, distributionField, numReducer, commitSize)
    })

    /**
     * This function runs the post commit action and finalizes the database tables.
     *
     * @param job The MR job object
     * @param driver The schedoscope driver
     * @param runState The job's runstate
     */
  def jdbcPostCommit(job: Job, driver: Driver[MapreduceTransformation], runState: DriverRunState[MapreduceTransformation]): DriverRunState[_] = {

    val jobConfigurer = new JdbcExportJob()

    try {
      jobConfigurer.postCommit(runState.isInstanceOf[DriverRunSucceeded[MapreduceTransformation]], job.getConfiguration)
      runState
    } catch {
      case ex: RetryException => throw new RetryableDriverException(ex.getMessage, ex)
      case ex: UnrecoverableException => DriverRunFailed.apply(driver, ex.getMessage, ex)
    }
  }

  /**
   * This function configures the Redis export job and returns a MapreduceTransformation.
   *
   * @param v The view
   * @param redisHost The Redis hostname
   * @param keyName The field to use as Redis key
   * @param redisPort The Redis port
   * @param redisKeySpace The Redis key space (default 0)
   * @param valueName An optional field for the native export
   * @param keyPrefix An optional key prefix
   * @param numReducer The number of reducer, defines concurrency
   * @param replace A flag indicating of exisiting should be replaced (or extended)
   * @param pipeline A flag indicating that the Redis pipeline mode should be used for writing data
   * @param flush A flag indicating if the key space should be flushed before writing data
   */
  def redisExport(v: View, redisHost: String, keyName: Field[_], redisPort: Int = 6379, redisKeySpace: Int = 0, valueName: Field[_] = null,
    keyPrefix: String = "", numReducer: Int = 10, replace: Boolean = false, pipeline: Boolean = false, flush: Boolean = true) =
    MapreduceTransformation(v, (config) => {

      val partitionParameters = v.partitionParameters
      val filter = partitionParameters.map { p => s"${p.n} = '${p.v.get}'" }.mkString(" and ")
      val valueField = if (valueName != null) valueName.n else null

      val settings = Settings.apply()
      val secure = if (settings.kerberosPrincipal.isEmpty) false else true
      val jobConfigurer = new RedisExportJob()

      jobConfigurer.configure(secure, settings.metastoreUri, settings.kerberosPrincipal, redisHost, redisPort, redisKeySpace, v.dbName,
        v.tableName, filter, keyName.n, valueField, keyPrefix, numReducer, replace, pipeline, flush)
    })
}
