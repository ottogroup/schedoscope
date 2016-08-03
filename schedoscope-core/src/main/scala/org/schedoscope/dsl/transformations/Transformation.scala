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

import org.schedoscope.dsl.View

import scala.collection.mutable.HashMap

/**
  * Base class for transformation types
  */
abstract class Transformation {
  /**
    * View to transformation belongs to
    */
  var view: Option[View] = None

  /**
    * Explicitly defined version to overwrite checksum
    */
  var definedVersion: Option[String] = None


  def getView() = if (view.isDefined) view.get.urlPath else "no-view"

  /**
    * Name of the transformation type.
    */
  def name: String

  /**
    * Configuration properties of transformation. Semantics depend on the transformation type. Usually stuff like query placeholders.
    */
  val configuration = HashMap[String, Any]()

  /**
    * Fluent interface to attach a configuration to a transformation.
    */
  def configureWith(c: Map[String, Any]) = {
    configuration ++= c
    this
  }

  /**
    * Fluent interface to define a version for the transformation.
    * The transformation will now not be invalidated by changes to the logic but only by changes to this version.
    * @param definedVersion string with version
    */
  def defineVersion(definedVersion: String) = {
    this.definedVersion = Some(definedVersion)
    this
  }

  /**
    * List of file resource paths that influence the transformation checksum of the transformation type.
    */
  def fileResourcesToChecksum = List[String]()

  /**
    * Other checksum influencing strings.
    */
  def stringsToChecksum = List[String]()

  /**
    * Transformation checksum. Per default an MD5 hash of the file resource hashes.
    */
  def checksum = definedVersion match {
    case Some(version) => Checksum.digest(version)
    case None => Checksum.digest((Checksum.resourceHashes(fileResourcesToChecksum) ++ stringsToChecksum): _*)
  }

  /**
    * Attach a transformation to a view.
    */
  def forView(v: View) = {
    view = Some(v)
    this
  }

  /**
    * A textual format for outputting a transformation
    */
  var description = this.toString

  /**
    * Used to validate the transformation during tests
    */
  @throws[InvalidTransformationException]
  def validateTransformation() = {}

}

/**
  * NoOp transformation type. Default transformation which does nothing but checking for
  * the existence of a _SUCCESS flag in the view's fullPath for materialization.
  */
case class NoOp() extends Transformation {
  override def name = "noop"
}

object Transformation {
  def replaceParameters(transformationSyntax: String, parameters: Map[String, Any]): String = {
    if (parameters.isEmpty)
      transformationSyntax
    else {
      val (key, value) = parameters.head
      val replacedStatement = transformationSyntax.replaceAll(java.util.regex.Pattern.quote("${" + key + "}"), value.toString().replaceAll("\\$", "|"))
      replaceParameters(replacedStatement, parameters.tail)
    }
  }
}

class InvalidTransformationException(msg: String) extends Throwable(msg)