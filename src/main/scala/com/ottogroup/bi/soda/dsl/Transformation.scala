package com.ottogroup.bi.soda.dsl

import scala.collection.mutable.HashMap

import com.ottogroup.bi.soda.bottler.api.Settings
import com.ottogroup.bi.soda.bottler.driver.FileSystemDriver

abstract class Transformation {
  var view: Option[View] = None

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

  def forView(v: View) = {
    view = Some(v)
    this
  }

  var description = this.toString

  def getView() = if (view.isDefined) view.get.urlPath else "no-view"

  def name: String
}

case class NoOp() extends Transformation {
  override def name = "noop"
}
