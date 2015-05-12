package org.schedoscope.dsl



import scala.collection.mutable.HashMap
import org.schedoscope.Settings
import org.schedoscope.scheduler.driver.FileSystemDriver

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

abstract class ExternalTransformation extends Transformation

case class NoOp() extends Transformation {
  override def name = "noop"
}

object Transformation {
  def replaceParameters(query: String, parameters: Map[String, Any]): String = {
    if (parameters.isEmpty)
      query
    else {
      val (key, value) = parameters.head
      val replacedStatement = query.replaceAll(java.util.regex.Pattern.quote("${" + key + "}"), value.toString().replaceAll("\\$", "|"))
      replaceParameters(replacedStatement, parameters.tail)
    }
  }
}
