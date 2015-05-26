/**
 * Copyright 2015 Otto (GmbH & Co KG)
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
package org.schedoscope.test.resources

import java.sql.DriverManager
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.security.UserGroupInformation
import org.schedoscope.scheduler.driver.HiveDriver
import org.schedoscope.scheduler.driver.PigDriver
import org.schedoscope.schema.SchemaManager
import org.schedoscope.dsl.TextFile
import org.schedoscope.test.Database
import java.sql.Connection
import org.schedoscope.scheduler.driver.OozieDriver
import org.schedoscope.scheduler.driver.MorphlineDriver
import org.apache.hadoop.conf.Configuration
import org.schedoscope.scheduler.driver.MapreduceDriver

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

  lazy val crate: SchemaManager = SchemaManager(metastoreClient, connection)

  lazy val hiveDriver: HiveDriver = new HiveDriver(ugi, jdbcUrl, metastoreClient) {
    override def JDBC_CLASS = jdbcClass
  }

  val fileSystem: FileSystem

  lazy val oozieDriver: OozieDriver = null

  lazy val pigDriver: PigDriver = new PigDriver(ugi)

  lazy val mapreduceDriver: MapreduceDriver = new MapreduceDriver(ugi)

  lazy val morphlineDriver = new MorphlineDriver(ugi, new Configuration(true))

  val remoteTestDirectory: String

  val namenode: String

  lazy val textStorage = new TextFile(fieldTerminator = "\\t", collectionItemTerminator = "&", mapKeyTerminator = "=")
}
