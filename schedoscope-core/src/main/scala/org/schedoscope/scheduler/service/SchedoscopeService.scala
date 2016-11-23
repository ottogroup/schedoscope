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
package org.schedoscope.scheduler.service


import scala.concurrent.Future


case class TransformationStatus(actor: String, typ: String, status: String, runStatus: Option[RunStatus], properties: Option[Map[String, String]])

case class TransformationStatusList(overview: Map[String, Int], transformations: List[TransformationStatus])

case class ViewStatus(
                       viewPath: String,
                       viewTableName: Option[String],
                       status: String,
                       properties: Option[Map[String, String]],
                       fields: Option[List[FieldStatus]],
                       parameters: Option[List[FieldStatus]],
                       dependencies: Option[Map[String, List[String]]],
                       transformation: Option[ViewTransformationStatus],
                       export: Option[List[ViewTransformationStatus]],
                       storageFormat: Option[String],
                       materializeOnce: Option[Boolean],
                       comment: Option[Option[String]],
                       isTable: Option[Boolean])

case class FieldStatus(name: String, fieldtype: String, comment: Option[String])

case class ViewTransformationStatus(name: String, properties: Option[Map[String, String]])

case class ViewStatusList(overview: Map[String, Int], views: List[ViewStatus])

case class QueueStatusList(overview: Map[String, Int], queues: Map[String, List[RunStatus]])

case class RunStatus(description: String, targetView: String, started: String, comment: String, properties: Option[Map[String, String]])


/**
  * Interface defining the functionality of the Schedoscope service. The services allows one to inject
  * scheduling commands into the Schedoscope actor system and obtain scheduling states or results from it.
  *
  * As most scheduling commands are performed asynchronously, the interface uses a SchedoscopeCommand case
  * class for representing an issued command along with SchedoscopeCommandStatus to represent the state of
  * the command.
  *
  * Analogously, the interface makes use of case classes for capturing view, transformation, and queue status.
  */
trait SchedoscopeService {

  /**
    * Materialize view(s). The views that are being materialized are selected either by
    * passing a view URL pattern, a status selector, a regexp filter on view URLs, or a combination of those.
    *
    * Additionally, a MaterializeViewMode can be passed.
    *
    * This method returns the initial status of the views being materialized.
    *
    * Throws an InvalidArgumentException if an invalid view URL pattern or regexp filter are passed.
    */
  def materialize(viewUrlPath: Option[String], status: Option[String], filter: Option[String], mode: Option[String]): Future[ViewStatusList]

  /**
    * Invalidate view(s). The views that are being invalidated are selected either by
    * passing a view URL pattern, a status selector, a regexp filter on view URLs, or a combination of those.
    *
    * Additionally, it can also be specified whether children are to be invalidated as well.
    *
    * This method returns the last status of the views that were invalidated.
    *
    * Throws an InvalidArgumentException if an invalid view URL pattern or regexp filter are passed.
    */
  def invalidate(viewUrlPath: Option[String], status: Option[String], filter: Option[String], dependencies: Option[Boolean]): Future[ViewStatusList]

  /**
    * Return view(s) and their state(s). The views for which states are being returned are selected either by
    * passing a view URL pattern, a status selector, a regexp filter on view URLs, or a combination of those.
    *
    * Additionally, it can also be specified whether view states should recursively carry the states of their dependendencies.
    *
    * Finally, there is the option to just return an overview count of views in states instead of returning the states themselves.
    *
    * Throws an InvalidArgumentException if an invalid view URL pattern or regexp filter are passed.
    */
  def views(viewUrlPath: Option[String], status: Option[String], filter: Option[String], dependencies: Option[Boolean], overview: Option[Boolean], all: Option[Boolean]): Future[ViewStatusList]

  /**
    * Return the states of the transformation drivers. Transformation driver info can be filtered by transformation state or a regexp
    * on the driver id.
    *
    * Throws an InvalidArgumentException if an invalid regexp filter is passed.
    */
  def transformations(status: Option[String], filter: Option[String]): Future[TransformationStatusList]

  /**
    * Returns the transformations waiting in queues. These can be filtered by transformation type or a regexp.
    *
    * Throws an InvalidArgumentException if an invalid regexp filter is passed.
    */
  def queues(typ: Option[String], filter: Option[String]): Future[QueueStatusList]

  /**
    * Shut down Schedoscope.
    */
  def shutdown(): Boolean
}