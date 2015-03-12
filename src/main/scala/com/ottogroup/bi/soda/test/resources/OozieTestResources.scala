package com.ottogroup.bi.soda.test.resources

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf

import com.ottogroup.bi.eci.minioozie.MiniOozie
import com.ottogroup.bi.soda.bottler.driver.OozieDriver

class OozieTestResources extends TestResources {
  val mo = new MiniOozie()

  override val jdbcClass = "org.apache.hive.jdbc.HiveDriver"

  override lazy val hiveConf: HiveConf = mo.getHiveServer2Conf

  override lazy val hiveWarehouseDir: String = mo.getFsTestCaseDir.toString

  override lazy val jdbcUrl = mo.getHiveServer2JdbcURL

  override lazy val remoteTestDirectory: String = mo.getFsTestCaseDir.toString

  override lazy val oozieDriver: OozieDriver = new OozieDriver(mo.getClient)

  override lazy val fileSystem: FileSystem = mo.getFileSystem

  override lazy val namenode = mo.getNameNodeUri
}