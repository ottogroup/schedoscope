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

case class SchedoscopeCommand(id: String, start: String, parts: List[Future[_]])
case class SchedoscopeCommandStatus(id: String, start: String, end: Option[String], status: Map[String, Int])
case class TransformationStatus(actor: String, typ: String, status: String, runStatus: Option[RunStatus], properties: Option[Map[String, String]])
case class TransformationStatusList(overview: Map[String, Int], transformations: List[TransformationStatus])
case class ViewStatus(view: String, status: String, properties: Option[Map[String, String]], dependencies: Option[List[ViewStatus]])
case class ViewStatusList(overview: Map[String, Int], views: List[ViewStatus])
case class QueueStatusList(overview: Map[String, Int], queues: Map[String, List[RunStatus]])
case class RunStatus(description: String, targetView: String, started: String, comment: String, properties: Option[Map[String, String]])

/**
 * Interface defining the functionality of the Schedoscope service. The services allows one to inject
 * scheduling commands into the Schedoscope actor system and obtain scheduling states or results from it.
 * As most scheduling commands are performed asynchronously, the interface uses a SchedoscopeCommand case
 * class for representing an issued command along with SchedoscopeCommandStatus to represent the state of the command.
 *
 * Analogously, the interface makes use of case classes for capturing view, transformation, and queue status.
 */
trait SchedoscopeService {

  def materialize(viewUrlPath: Option[String], status: Option[String], filter: Option[String], mode: Option[String]): SchedoscopeCommandStatus

  def invalidate(viewUrlPath: Option[String], status: Option[String], filter: Option[String], dependencies: Option[Boolean]): SchedoscopeCommandStatus

  def newdata(viewUrlPath: Option[String], status: Option[String], filter: Option[String]): SchedoscopeCommandStatus

  def commandStatus(commandId: String): SchedoscopeCommandStatus

  def commands(status: Option[String], filter: Option[String]): List[SchedoscopeCommandStatus]

  def views(viewUrlPath: Option[String], status: Option[String], filter: Option[String], dependencies: Option[Boolean], overview: Option[Boolean]): ViewStatusList

  def transformations(status: Option[String], filter: Option[String]): TransformationStatusList

  def queues(typ: Option[String], filter: Option[String]): QueueStatusList

  def shutdown(): Boolean
}