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
package org.schedoscope.scheduler

import org.schedoscope.scheduler.driver.Driver
import org.schedoscope.scheduler.driver.DriverRunFailed
import org.schedoscope.scheduler.driver.DriverRunHandle
import org.schedoscope.scheduler.driver.DriverRunState
import org.schedoscope.scheduler.driver.DriverRunSucceeded
import org.schedoscope.dsl.Transformation
import org.schedoscope.dsl.View

import akka.actor.ActorRef

sealed class Failure
case class Error(view: View, reason: String) extends Failure
case class Failed(view: View) extends Failure
case class ActionFailure[T <: Transformation](driverRunHandle: DriverRunHandle[T], driverRunState: DriverRunFailed[T]) extends Failure

sealed class CommandRequest
case class AddPartitions(views: List[View]) extends CommandRequest
case class CheckOrCreateTables(views: List[View]) extends CommandRequest
case class NewDataAvailable(view: View) extends CommandRequest
case class KillAction() extends CommandRequest
case class Suspend() extends CommandRequest
case class Deploy() extends CommandRequest
case class Retry() extends CommandRequest
case class Invalidate() extends CommandRequest
case class PollCommand(typ: String) extends CommandRequest
case class CommandWithSender(command: AnyRef, sender: ActorRef) extends CommandRequest
case class SetViewVersion(view: View) extends CommandRequest
case class LogTransformationTimestamp(view: View, timestamp: Long) extends CommandRequest
case class GetActions() extends CommandRequest
case class GetQueues() extends CommandRequest
case class GetViews(views: Option[List[View]], status: Option[String], filter: Option[String], dependencies: Boolean = false)
case class GetActionStatusList(statusRequester: ActorRef, actionQueueStatus: Map[String, List[String]], driverActors: Seq[ActorRef]) extends CommandRequest
case class GetViewStatusList(statusRequester: ActorRef, viewActors: Iterable[ActorRef]) extends CommandRequest
case class MaterializeView(mode: String = MaterializeViewMode.default) extends CommandRequest
object MaterializeViewMode { val default = "DEFAULT"; val resetTransformationChecksums = "RESET_TRANSFORMATION_CHECKSUMS" }

sealed class CommandResponse
case class DeployActionSuccess() extends CommandResponse
case class SchemaActionSuccess() extends CommandResponse
case class ActionSuccess[T <: Transformation](driverRunHandle: DriverRunHandle[T], driverRunState: DriverRunSucceeded[T]) extends CommandResponse
case class QueueStatusListResponse(val actionQueues: Map[String, List[AnyRef]]) extends CommandResponse
case class ActionStatusListResponse(val actionStatusList: List[ActionStatusResponse[_]]) extends CommandResponse
case class ActionStatusResponse[T <: Transformation](val message: String, val actor: ActorRef, val driver: Driver[T], driverRunHandle: DriverRunHandle[T], driverRunStatus: DriverRunState[T]) extends CommandResponse
case class ViewStatusResponse(val status: String, view: View, actor: ActorRef) extends CommandResponse
case class ViewStatusListResponse(viewStatusList: List[ViewStatusResponse]) extends CommandResponse
case class ViewVersionOk(view: View) extends CommandResponse
case class ViewVersionMismatch(view: View, dataVersion: String) extends CommandResponse
case class TransformationTimestamp(view: View, timestamp: Long) extends CommandResponse
case class TransformationMetadata(metadata: Map[View, (String, Long)]) extends CommandResponse
case class NoDataAvailable(view: View) extends CommandResponse
case class ViewMaterialized(view: View, incomplete: Boolean, transformationTimestamp: Long, errors: Boolean) extends CommandResponse

