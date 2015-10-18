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
package org.schedoscope.dsl

import java.security.MessageDigest
import scala.Array.canBuildFrom
import org.schedoscope.scheduler.driver.FileSystemDriver
import org.schedoscope.Settings
import org.schedoscope.dsl.storageformats._

object Version {
  def md5 = MessageDigest.getInstance("MD5")

  val fsd = FileSystemDriver(Settings().getDriverSettings("filesystem"))

  val default = "0"

  def digest(s: String): String = digest(List(s))

  def resourceHashes(resources: List[String]): List[String] =
    fsd.fileChecksums(resources, true)

  def digest(strings: List[String]): String = if (strings.isEmpty)
    default
  else
    md5.digest(strings.sorted.mkString.toCharArray().map(_.toByte)).map("%02X" format _).mkString

  object SchemaVersion {
    def checksumProperty() = "schema.checksum"
  }

  object TransformationVersion {
    def checksumProperty() = "transformation.checksum"
    def timestampProperty() = "transformation.timestamp"
  }
}

