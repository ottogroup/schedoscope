package com.ottogroup.bi.soda.test.resources

import com.ottogroup.bi.eci.minioozie.MiniOozie
import org.apache.hadoop.hive.conf.HiveConf
import java.sql.Connection
import java.sql.DriverManager
import com.ottogroup.bi.soda.bottler.driver.FileSystemDriver
import com.ottogroup.bi.soda.bottler.driver.OozieDriver
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import com.ottogroup.bi.soda.test.Database
import com.ottogroup.bi.soda.crate.DeploySchema
import com.ottogroup.bi.soda.bottler.driver.HiveDriver
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf.ConfVars._
import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.Paths
import java.util.Properties
import collection.JavaConversions._

class OozieTestResources extends TestResources {

  val mo = new MiniOozie()

  override val hiveConf: HiveConf = mo.getHiveServer2Conf

  override val hiveWarehouseDir: String = mo.getFsTestCaseDir.toString

  override val connection: Connection = {
    Class.forName(HIVE2_JDBC_CLASS)
    //EnvironmentHack.setEnv(Map(("CLASSPATH", "")))
    val conn = DriverManager.getConnection(mo.getHiveServer2JdbcURL, "", "")
    //conn.createStatement().execute("SET system:java.class.path=")
    conn
  }

  override val localTestDirectory: String = "" // TODO

  override val remoteTestDirectory: String = mo.getFsTestCaseDir.toString

  override val fileSystemDriver: FileSystemDriver = null // TODO

  override val oozieDriver: OozieDriver = new OozieDriver(mo.getClient)

  override val fileSystem: FileSystem = mo.getFileSystem

  override val metastoreClient: HiveMetaStoreClient = new HiveMetaStoreClient(hiveConf)
  override val database: Database = new Database(connection, mo.getHiveServer2JdbcURL)
  override val bottler: DeploySchema = DeploySchema(metastoreClient, connection)
  override val hiveDriver: HiveDriver = new HiveDriver(connection)
}