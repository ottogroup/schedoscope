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
  def hiveConf: HiveConf

  def hiveWarehouseDir: String

  private var cachedConnection: Connection = null
  def connection: Connection = {
    if (cachedConnection == null) {
      val c = hiveConf //make sure config is written before JDBC connection is established
      Class.forName(jdbcClass)
      cachedConnection = DriverManager.getConnection(jdbcUrl, "", "")
    }

    cachedConnection
  }

  private var cachedUgi: UserGroupInformation = null
  def ugi: UserGroupInformation = {
    if (cachedUgi == null) {
      UserGroupInformation.setConfiguration(hiveConf)
      cachedUgi = UserGroupInformation.getCurrentUser()
      cachedUgi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS)
      cachedUgi.reloginFromKeytab();
    }

    cachedUgi
  }

  private var cachedMetastoreClient: HiveMetaStoreClient = null
  def metastoreClient: HiveMetaStoreClient = {
    if (cachedMetastoreClient == null) {
      cachedMetastoreClient = new HiveMetaStoreClient(hiveConf)
    }
    cachedMetastoreClient
  }

  def jdbcClass: String

  def jdbcUrl: String

  private var cachedDatabase: Database = null
  def database: Database = {
    if (cachedDatabase == null)
      cachedDatabase = new Database(connection, jdbcUrl)

    cachedDatabase
  }

  private var cachedBottler: DeploySchema = null
  def bottler: DeploySchema = {
    if (cachedBottler == null)
      cachedBottler = DeploySchema(metastoreClient, connection)

    cachedBottler
  }

  private var cachedHiveDriver: HiveDriver = null
  def hiveDriver: HiveDriver = {
    if (cachedHiveDriver == null)
      cachedHiveDriver = new HiveDriver(ugi, jdbcUrl, metastoreClient) {
        override def JDBC_CLASS = jdbcClass
      }

    cachedHiveDriver
  }

  def fileSystem: FileSystem

  def oozieDriver: OozieDriver = null

  def remoteTestDirectory: String

  def namenode: String

  private var cachedTextStore: TextFile = null
  def textStorage = {
    if (cachedTextStore == null)
      cachedTextStore = new TextFile(fieldTerminator = "\\t", collectionItemTerminator = "&", mapKeyTerminator = "=")

    cachedTextStore
  }
}