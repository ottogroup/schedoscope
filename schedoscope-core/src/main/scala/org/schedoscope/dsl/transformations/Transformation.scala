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
import org.schedoscope.scheduler.service.ViewTransformationStatus
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import scala.collection.mutable.HashMap

/**
 * Base class for transformation types
 */
abstract class Transformation {
  /**
   * View to transformation belongs to
   */
  var view: Option[View] = None

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
  lazy val checksum = Checksum.digest((Checksum.resourceHashes(fileResourcesToChecksum) ++ stringsToChecksum): _*)

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

}

/**
 * Base class for external transformation types, i.e., transformations happening outside the HADOOP cluster.
 */
abstract class ExternalTransformation extends Transformation

/**
 * NoOp transformation type. Default transformation which does nothing but checking for
 * the existence of a _SUCCESS flag in the view's fullPath for materialization.
 */
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
  
    def stringRepresentation(transformation: Transformation): ViewTransformationStatus = {
    transformation match {
      case t: HiveTransformation => ViewTransformationStatus(t.name, Some(Map("sql" -> t.sql)));
      case t: MapreduceTransformation => ViewTransformationStatus(t.name, 
          Some(Map("input" -> t.job.getConfiguration().get(FileInputFormat.INPUT_DIR),
              "output" -> t.job.getConfiguration().get(FileOutputFormat.OUTDIR))));
      case t: PigTransformation => ViewTransformationStatus(t.name, Some(Map("latin" -> t.latin)));
      case t: OozieTransformation => ViewTransformationStatus(t.name, Some(Map("bundle" -> t.bundle, "workflow" -> t.workflow)));
      case t: ShellTransformation => ViewTransformationStatus(t.name, Some(Map("shell" -> t.shell, "script" -> t.script,
              "scriptFile" -> t.scriptFile) ++ t.env));
      case t: CopyFrom => ViewTransformationStatus("filesystem -> CopyFromTransformation", Some(Map("from" -> t.fromPattern,
              "destinationView" -> t.toView.urlPath, "recursive" -> t.recursive.toString())));
      case t: Copy => ViewTransformationStatus("filesystem -> CopyTransformation", Some(Map("from" -> t.fromPattern,
              "destinationPath" -> t.toPath)));
      case t: Move => ViewTransformationStatus("filesystem -> MoveTransformation", Some(Map("from" -> t.fromPattern,
              "destinationPath" -> t.toPath)));
      case t: StoreFrom => ViewTransformationStatus("filesystem -> StoreFromTransformation", Some(Map("destinationView" -> t.toView.urlPath)));
      case t => ViewTransformationStatus(t.name, None);
    }
  }
}
