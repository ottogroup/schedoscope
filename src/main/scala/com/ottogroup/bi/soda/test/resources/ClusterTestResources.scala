package com.ottogroup.bi.soda.test.resources

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf

import com.ottogroup.bi.eci.minioozie.MiniOozie
import com.ottogroup.bi.soda.bottler.driver.OozieDriver

class OozieTestResources extends TestResources {
  val mo = new MiniOozie()

  override def jdbcClass = "org.apache.hive.jdbc.HiveDriver"

  override def hiveConf: HiveConf = mo.getHiveServer2Conf

  override def hiveWarehouseDir: String = mo.getFsTestCaseDir.toString

  override def jdbcUrl = mo.getHiveServer2JdbcURL

  override def remoteTestDirectory: String = mo.getFsTestCaseDir.toString

  var cachedOozieDriver: OozieDriver = null
  override def oozieDriver: OozieDriver = {
    if (cachedOozieDriver == null)
      cachedOozieDriver = new OozieDriver(mo.getClient)

    cachedOozieDriver
  }

  override def fileSystem: FileSystem = mo.getFileSystem

  override def namenode = mo.getNameNodeUri
}