package org.schedoscope.dsl.transformations

import org.apache.hadoop.mapreduce.Job
import org.schedoscope.Settings
import org.schedoscope.dsl.{ Field, View }
import org.schedoscope.export.jdbc.JdbcExportJob
import org.schedoscope.export.jdbc.exception.{ RetryException, UnrecoverableException }
import org.schedoscope.export.redis.RedisExportJob
import org.schedoscope.scheduler.driver.{ Driver, DriverRunFailed, DriverRunState, DriverRunSucceeded, RetryableDriverException }

object Export {

  def jdbcExport(v: View, dbConn: String, dbUser: String, dbPass: String, storageEngine: String = "InnoDB", distributionKey: Field[_], numReducer: Int = 10, commitSize: Int = 10000) =
    MapreduceTransformation(v, (config) => {

      val partitionParameters = v.partitionParameters
      val filter = partitionParameters.map { p => s"${p.n} = '${p.v.get}'" }
        .mkString(" and ")

      val settings = Settings.apply()
      val secure = if (settings.kerberosPrincipal.isEmpty) false else true
      val jobConfigurer = new JdbcExportJob()

      jobConfigurer.configure(secure, settings.metastoreUri, settings.kerberosPrincipal, dbConn, dbUser, dbPass,
        v.dbName, v.tableName, filter, storageEngine, distributionKey.n, numReducer, commitSize)
    })

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
