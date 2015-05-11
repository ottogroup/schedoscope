package org.schedoscope.test.resources

import minioozie.MiniOozie
import com.ottogroup.bi.soda.bottler.driver.OozieDriver
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.fs.FileSystem

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

object OozieTestResources {
  lazy val oozieTestResources = new OozieTestResources()
  
  def apply() = oozieTestResources
}