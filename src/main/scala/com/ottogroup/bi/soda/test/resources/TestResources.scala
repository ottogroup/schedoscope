package com.ottogroup.bi.soda.test.resources

import org.apache.hadoop.hive.conf.HiveConf
import java.sql.Connection
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import com.ottogroup.bi.soda.test.Database
import com.ottogroup.bi.soda.crate.DeploySchema
import com.ottogroup.bi.soda.bottler.driver.HiveDriver
import com.ottogroup.bi.soda.bottler.driver.FileSystemDriver
import com.ottogroup.bi.soda.bottler.driver.OozieDriver
import com.ottogroup.bi.soda.dsl.TextFile
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.UserGroupInformation

abstract class TestResources {
  val hiveConf: HiveConf
  val hiveWarehouseDir: String
  val connection: Connection
  val ugi: UserGroupInformation
  val metastoreClient: HiveMetaStoreClient
  val database: Database
  val bottler: DeploySchema
  val hiveDriver: HiveDriver
  val fileSystemDriver: FileSystemDriver
  val fileSystem: FileSystem
  val oozieDriver: OozieDriver
  val localTestDirectory: String
  val remoteTestDirectory: String
  val namenode: String
  final val textStorage = new TextFile(fieldTerminator = "\\t", collectionItemTerminator = "&", mapKeyTerminator = "=")
  final val LOCAL: String = "local"
  final val OOZIE: String = "oozie"
  final val HIVE_JDBC_CLASS = "org.apache.hadoop.hive.jdbc.HiveDriver"
  final val HIVE2_JDBC_CLASS = "org.apache.hive.jdbc.HiveDriver"
}