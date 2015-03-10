package com.ottogroup.bi.soda.dsl

import scala.collection.mutable.HashMap
import com.ottogroup.bi.soda.bottler.driver.FileSystemDriver
import com.ottogroup.bi.soda.bottler.api.Settings

abstract class Transformation {
  // FIXME: not so nice that each transformation has the file system driver .. 
  val fsd = FileSystemDriver(Settings().getDriverSettings("filesystem"))

  def configureWith(c: Map[String, Any]) = {
    configuration ++= c
    this
  }
  
  val configuration = HashMap[String, Any]()

  def versionDigest() = Version.digest(resourceHashes)

  def resources() = List[String]()

  def resourceHashes = fsd.fileChecksums(resources(), true)

  def typ = this.getClass.getSimpleName.toLowerCase.replaceAll("transformation", "")
}

case class NoOp() extends Transformation
