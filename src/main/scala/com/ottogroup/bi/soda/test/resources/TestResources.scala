package com.ottogroup.bi.soda.test.resources

import java.sql.Connection
import java.sql.DriverManager

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.security.UserGroupInformation

import com.ottogroup.bi.soda.bottler.driver.HiveDriver
import com.ottogroup.bi.soda.bottler.driver.OozieDriver
import com.ottogroup.bi.soda.crate.DeploySchema
import com.ottogroup.bi.soda.dsl.TextFile
import com.ottogroup.bi.soda.test.Database

abstract class TestResources {
  val hiveConf: HiveConf

  val hiveWarehouseDir: String

  lazy val connection: Connection = {
    val c = hiveConf
    Class.forName(jdbcClass)
    DriverManager.getConnection(jdbcUrl, "", "")
  }

  lazy val ugi: UserGroupInformation = {
    UserGroupInformation.setConfiguration(hiveConf)
    val ugi = UserGroupInformation.getCurrentUser()
    ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS)
    ugi.reloginFromKeytab()
    ugi
  }

  lazy val metastoreClient: HiveMetaStoreClient = new HiveMetaStoreClient(hiveConf)

  val jdbcClass: String

  val jdbcUrl: String

  lazy val database = new Database(connection, jdbcUrl)

  lazy val bottler: DeploySchema = DeploySchema(metastoreClient, connection)

  lazy val hiveDriver: HiveDriver = new HiveDriver(ugi, jdbcUrl, metastoreClient) {
    override def JDBC_CLASS = jdbcClass
    override def name = "hive"
  }

  val fileSystem: FileSystem

  lazy val oozieDriver: OozieDriver = null

  val remoteTestDirectory: String

  val namenode: String

  lazy val textStorage = new TextFile(fieldTerminator = "\\t", collectionItemTerminator = "&", mapKeyTerminator = "=")
}