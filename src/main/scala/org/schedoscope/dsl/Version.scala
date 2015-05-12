package org.schedoscope.dsl

import java.security.MessageDigest
import scala.Array.canBuildFrom
import com.ottogroup.bi.soda.bottler.driver.FileSystemDriver
import com.ottogroup.bi.soda.Settings

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

