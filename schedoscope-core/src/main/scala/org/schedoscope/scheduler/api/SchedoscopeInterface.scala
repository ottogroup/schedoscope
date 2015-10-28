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
package org.schedoscope.scheduler.api

trait SchedoscopeInterface {

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