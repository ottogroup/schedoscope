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
package org.schedoscope.dsl.transformations

import java.security.MessageDigest

import org.apache.hadoop.fs.{FileStatus, Path}
import org.schedoscope.Schedoscope
import org.schedoscope.scheduler.driver.FilesystemDriver._

import scala.Array.canBuildFrom
import scala.collection.mutable.HashMap

object Checksum {
  private def md5 = MessageDigest.getInstance("MD5")

  private def listFiles(path: String): Array[FileStatus] = {
    val files = fileSystem(path, Schedoscope.settings.hadoopConf).globStatus(new Path(path))
    if (files != null)
      files
    else Array()
  }

  private def fileChecksum(path: String) =
    if (path == null)
      "null-checksum"
    else if (path.endsWith(".jar"))
      path
    else try {
      val cs = fileSystem(path, Schedoscope.settings.hadoopConf).getFileChecksum(new Path(path))
      if (cs == null)
        path
      else
        cs.toString()
    } catch {
      case _: Throwable => path
    }

  def fileChecksums(paths: List[String], recursive: Boolean): List[String] =
    paths.flatMap(path => {
      if (fileSystem(path, Schedoscope.settings.hadoopConf).isFile(new Path(path)))
        List(fileChecksum(path))
      else if (recursive)
        fileChecksums(listFiles(path + "/*").map(f => f.getPath.toString()).toList, recursive)
      else
        List()
    }).sorted

  val resourceHashCache = new HashMap[List[String], List[String]]()

  def resourceHashes(resources: List[String]): List[String] = synchronized {
    resourceHashCache.getOrElseUpdate(resources, fileChecksums(resources, true))
  }

  val defaultDigest = "0"

  def digest(stringsToDigest: String*): String =
    if (stringsToDigest.isEmpty)
      defaultDigest
    else
      md5.digest(stringsToDigest.sorted.mkString.toCharArray().map(_.toByte)).map("%02X" format _).mkString

  object SchemaChecksum {
    val checksumProperty = "schema.checksum"
  }

  object TransformationChecksum {
    val checksumProperty = "transformation.checksum"
    val timestampProperty = "transformation.timestamp"
  }

}

