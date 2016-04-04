package org.schedoscope.dsl.transformations

import org.schedoscope.dsl.{ Field, View }
import org.schedoscope.export.jdbc.JdbcExportJob
import org.schedoscope.Settings
import org.apache.hadoop.mapreduce.Job
import org.schedoscope.scheduler.driver.DriverRunState
import org.schedoscope.scheduler.driver.DriverRunSucceeded

object Export {

  def jdbcExport(v: View, dbConn: String, dbUser: String, dbPass: String, storageEngine: String = "InnoDB", distributionKey: Field[_], numReducer: Int = 10, commitSize: Int = 10000) =
    MapreduceTransformation(v, (config) => {

      val partitionValues = v.partitionValues()
      val settings = Settings.apply()

      var secure = if (settings.kerberosPrincipal.isEmpty) false else true

      val jobConfigurer = new JdbcExportJob()

      jobConfigurer.configure(secure, settings.metastoreUri, settings.kerberosPrincipal, dbConn, dbUser, dbPass,
        v.dbName, v.tableName, "", storageEngine, distributionKey.n, numReducer, commitSize)
    })

  def jdbcPostCommit(runState: DriverRunState[_], job: Job) = {

    val jobConfigurer = new JdbcExportJob()
    jobConfigurer.postCommit(runState.isInstanceOf[DriverRunSucceeded[_]], job.getConfiguration)
  }
}
