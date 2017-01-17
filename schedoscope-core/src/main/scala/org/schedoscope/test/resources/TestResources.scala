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

import java.sql.{Connection, DriverManager}
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.security.UserGroupInformation
import org.schedoscope.dsl.storageformats.TextFile
import org.schedoscope.dsl.transformations._
import org.schedoscope.scheduler.driver.{Driver, DriverRunCompletionHandler, DriverRunHandle, DriverRunState}
import org.schedoscope.schema.SchemaManager
import org.schedoscope.test.Database

object TestDriverRunCompletionHandlerCallCounter {
  val driverRunStartedCalls = Collections.newSetFromMap(new ConcurrentHashMap[DriverRunHandle[_], java.lang.Boolean]())
  val driverRunCompletedCalls = new ConcurrentHashMap[DriverRunHandle[_], DriverRunState[_]]()

  def countDriverRunStartedHandlerCall(run: DriverRunHandle[_]) {
    driverRunStartedCalls.add(run)
  }

  def countDriverRunCompletedHandlerCall(run: DriverRunHandle[_], stateOfCompletion: DriverRunState[_]) {
    driverRunCompletedCalls.put(run, stateOfCompletion)
  }

  def driverRunStartedCalled(run: DriverRunHandle[_]) = driverRunStartedCalls.contains(run)

  def driverRunCompletedCalled(run: DriverRunHandle[_], stateOfCompletion: DriverRunState[_]) = driverRunCompletedCalls.get(run) == stateOfCompletion
}

class TestDriverRunCompletionHandler[T <: Transformation] extends DriverRunCompletionHandler[T] {

  import TestDriverRunCompletionHandlerCallCounter._

  def driverRunStarted(run: DriverRunHandle[T]) {
    countDriverRunStartedHandlerCall(run)
  }

  def driverRunCompleted(stateOfCompletion: DriverRunState[T], run: DriverRunHandle[T]) {
    countDriverRunCompletedHandlerCall(run, stateOfCompletion)
  }
}

/**
  * Keeps various properties for the test environment.
  */
abstract class TestResources {
  val hiveConf: HiveConf

  val hiveWarehouseDir: String

  val hiveScratchDir: String

  val hiveSiteXmlPath: Option[String]

  val metastoreUri: String

  val fileSystem: FileSystem

  val jdbcUrl: String

  val remoteTestDirectory: String

  val namenode: String

  lazy val ugi: UserGroupInformation = {
    UserGroupInformation.setConfiguration(hiveConf)
    val ugi = UserGroupInformation.getCurrentUser
    ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS)
    ugi.reloginFromKeytab()
    ugi
  }

  val jdbcClass: String = "org.apache.hive.jdbc.HiveDriver"

  lazy val connection: Connection = {
    val c = hiveConf
    Class.forName(jdbcClass)
    DriverManager.getConnection(jdbcUrl, "", "")
  }

  lazy val metastoreClient: HiveMetaStoreClient = new HiveMetaStoreClient(hiveConf)

  lazy val database = new Database(connection, jdbcUrl)

  lazy val schemaManager: SchemaManager = SchemaManager(metastoreClient, connection)

  def driverFor[T <: Transformation](driverName: String) = Driver.driverFor[T](driverName, Some(this))

  def driverFor[T <: Transformation](transformation: T) = Driver.driverFor[T](transformation, Some(this))

  lazy val textStorage = new TextFile(fieldTerminator = "\\t", collectionItemTerminator = "\u0002", mapKeyTerminator = "\u0003")
}
