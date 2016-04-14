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

import java.sql.{ Connection, DriverManager }
import java.util.concurrent.ConcurrentHashMap
import java.util.Collections

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.security.UserGroupInformation
import org.schedoscope.dsl.storageformats.TextFile
import org.schedoscope.dsl.transformations.Transformation
import org.schedoscope.scheduler.driver.{ Driver, DriverRunCompletionHandler, DriverRunHandle, DriverRunState, FileSystemDriver, HiveDriver, MapreduceDriver, OozieDriver, PigDriver, ShellDriver, SeqDriver }
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

abstract class TestResources {
  val hiveConf: HiveConf

  val hiveWarehouseDir: String

  val hiveScratchDir: String

  val metastoreUri: String

  val fileSystem: FileSystem

  val jdbcUrl: String

  val remoteTestDirectory: String

  val namenode: String

  lazy val ugi: UserGroupInformation = {
    UserGroupInformation.setConfiguration(hiveConf)
    val ugi = UserGroupInformation.getCurrentUser()
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

  lazy val crate: SchemaManager = SchemaManager(metastoreClient, connection)

  def driverFor(transformationName: String): Driver[Transformation] = (transformationName match {

    case "filesystem" => new FileSystemDriver(List("org.schedoscope.test.resources.TestDriverRunCompletionHandler"), ugi, new Configuration(true))

    case "pig"        => new PigDriver(List("org.schedoscope.test.resources.TestDriverRunCompletionHandler"), ugi)

    case "mapreduce"  => new MapreduceDriver(List("org.schedoscope.test.resources.TestDriverRunCompletionHandler"), ugi, new FileSystemDriver(List("org.schedoscope.test.resources.TestDriverRunCompletionHandler"), ugi, new Configuration(true)))

    case "shell"      => new ShellDriver(List("org.schedoscope.test.resources.TestDriverRunCompletionHandler"))

    case "hive"       => new HiveDriver(List("org.schedoscope.test.resources.TestDriverRunCompletionHandler"), ugi, jdbcUrl, metastoreClient)

    case "seq"        => new SeqDriver(List("org.schedoscope.test.resources.TestDriverRunCompletionHandler"), driverFor)

  }).asInstanceOf[Driver[Transformation]]

  lazy val fileSystemDriver: FileSystemDriver = driverFor("filesystem").asInstanceOf[FileSystemDriver]

  lazy val oozieDriver: OozieDriver = driverFor("oozie").asInstanceOf[OozieDriver]

  lazy val pigDriver: PigDriver = driverFor("pig").asInstanceOf[PigDriver]

  lazy val hiveDriver: HiveDriver = driverFor("hive").asInstanceOf[HiveDriver]

  lazy val mapreduceDriver: MapreduceDriver = driverFor("mapreduce").asInstanceOf[MapreduceDriver]

  lazy val shellDriver: ShellDriver = driverFor("shell").asInstanceOf[ShellDriver]

  lazy val seqDriver: SeqDriver = driverFor("seq").asInstanceOf[SeqDriver]

  lazy val textStorage = new TextFile(fieldTerminator = "\\t", collectionItemTerminator = "\u0002", mapKeyTerminator = "\u0003")
}
