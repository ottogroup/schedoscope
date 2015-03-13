package com.ottogroup.bi.soda.dsl

import java.security.MessageDigest

import scala.Array.canBuildFrom

/**
 * @author dev_dbenz
 */
object Version {

  val md5 = MessageDigest.getInstance("MD5")

  val default = "0"

  def digest(strings: String*): String = {
    if (strings.size == 0)
      default
    md5.digest(strings.mkString.toCharArray().map(_.toByte)).map("%02X" format _).mkString
  }

  def digest(strings: List[String]): String = digest(strings: _*)

  def check(v: String): String = {
    if (v == null)
      default
    v
  }

}

object SchemaVersion {
  def checksumProperty() = "schema.checksum"
}

object TransformationVersion {
  def checksumProperty() = "transformation.checksum"
}