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

import minioozie.MiniOozie
import org.schedoscope.scheduler.driver.OozieDriver
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.fs.FileSystem

class OozieTestResources extends TestResources {
  val mo = new MiniOozie()

  override val jdbcClass = "org.apache.hive.jdbc.HiveDriver"

  override lazy val hiveConf: HiveConf = mo.getHiveServer2Conf

  override lazy val hiveWarehouseDir: String = mo.getFsTestCaseDir.toString
  override lazy val hiveScratchDir: String = mo.getScratchDir().toString()
  

  override lazy val jdbcUrl = mo.getHiveServer2JdbcURL

  override lazy val remoteTestDirectory: String = mo.getFsTestCaseDir.toString

  override lazy val oozieDriver: OozieDriver = new OozieDriver(List("org.schedoscope.test.resources.TestDriverRunCompletionHandler"), mo.getClient)

  override lazy val fileSystem: FileSystem = mo.getFileSystem

  override lazy val namenode = mo.getNameNodeUri
}

object OozieTestResources {
  lazy val oozieTestResources = new OozieTestResources()

  def apply() = oozieTestResources
}