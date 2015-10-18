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
package org.schedoscope.scheduler.messages

import org.schedoscope.scheduler.driver.Driver
import org.schedoscope.scheduler.driver.DriverRunFailed
import org.schedoscope.scheduler.driver.DriverRunHandle
import org.schedoscope.scheduler.driver.DriverRunState
import org.schedoscope.scheduler.driver.DriverRunSucceeded
import org.schedoscope.dsl.Transformation
import org.schedoscope.dsl.View

import akka.actor.ActorRef

/**
 *  Superclass for failure messages. can be temporal or permanent failes.
 */
sealed class Failure

/**
 *  denotes a tempral failure or error
 */

case class Error(view: View, reason: String) extends Failure

/**
 * notifies of a permanent failure while transforming this view
 */
case class Failed(view: View) extends Failure

/**
 * notification of an error that stems from inside a driver
 */
case class ActionFailure[T <: Transformation](driverRunHandle: DriverRunHandle[T], driverRunState: DriverRunFailed[T]) extends Failure

/**
 * Defines the API for the SchemaActor @see SchemaActor
 * @todo
 */
sealed class SchemaCommandRequest

/**
 * Messages of type CommandRequest are the main API of the ActionsManagerActor and SchemaActor and will be
 * handled there.
 */
sealed class CommandRequest
/**
 * Instructs the SchemaActor to verify or create all Partitions that are specified in any
 * of the views in paramater 'views'
 */
case class AddPartitions(views: List[View]) extends CommandRequest
/**
 * Instructs the Schemaactor to verify or create all tables defined in the specified list of views
 */
case class CheckOrCreateTables(views: List[View]) extends CommandRequest
/**
 * Message is sent to all dependend views to notify that new data has arrived in the given
 * View. They need to decide whether to recompute themselvers
 */
case class NewDataAvailable(view: View) extends CommandRequest
/**
 * Command to interrupt a running action. Actual outcome depends on the ability of the
 * associated driver to be able to do that
 */
case class KillAction() extends CommandRequest
/**
 * Command to suspend a running action. Actual outcome depends on the ability of the
 * associated driver to be able to do that
 */
case class Suspend() extends CommandRequest
/**
 * Command to instruct all drivers to deploy their resources (e.g. UDFS) to the distributed file system
 */
case class Deploy() extends CommandRequest
/**
 * Instructs a view-actor to retry a transformation after a failure
 */
case class Retry() extends CommandRequest
/**
 * Instructs a view actor to assume that its data needs to be recomputed. Will result in
 * a state change.
 */
case class Invalidate() extends CommandRequest
/**
 * Polls the ActionsManagerActor for a new piece of work to be executed. Sent from DriverActor
 * to ActionsManagerActor
 */
case class PollCommand(typ: String) extends CommandRequest
/**
 * Encapsulates a command with its sender. This allows the ActionsManager to distributed work together
 * with the reference to the actor that requested the action. The sender can then be notified directly
 * about the outcome
 */
case class CommandWithSender(command: AnyRef, sender: ActorRef) extends CommandRequest
/**
 * Command to the MetadataLoggerActor to record the version information of the given view
 * in the metadata store
 */
case class SetViewVersion(view: View) extends CommandRequest
/**
 * Command to the MetadataLoggerActor to record the timestamp information of the last computation of a given view
 * in the metadata store
 */
case class LogTransformationTimestamp(view: View, timestamp: Long) extends CommandRequest
/**
 * Request to the ActionsManager to generate a summary of currently running actions
 */
case class GetActions() extends CommandRequest
/**
 * Request to the ActionsManger to retrieve the contents and size of the  worker queues
 */
case class GetQueues() extends CommandRequest
/**
 * Request to the ViewManagerActor to retrieve information of the currently instanciated views
 * @constructor
 * @param views  A list of views to retrieve information from, may be empty
 * @param status filter the result by view status
 * @param filter filter the result by regular expression on the view name
 * @param dependencies also return all dependent views
 */
case class GetViews(views: Option[List[View]], status: Option[String], filter: Option[String], dependencies: Boolean = false)
/**
 * Request to the ActionManager to return the state of all drivers
 */
case class GetActionStatusList(statusRequester: ActorRef, actionQueueStatus: Map[String, List[String]], driverActors: Seq[ActorRef]) extends CommandRequest
/**
 *  Request the exact state of a list of given views
 */
case class GetViewStatusList(statusRequester: ActorRef, viewActors: Iterable[ActorRef]) extends CommandRequest

/**
 *  Flags for the MaterializeView command
 */
object MaterializeViewMode extends Enumeration {
  type MaterializeViewMode = Value

  val DEFAULT, // no special mode
  RESET_TRANSFORMATION_CHECKSUMS, // don't transform, just update the checksums
  RESET_TRANSFORMATION_CHECKSUMS_AND_TIMESTAMPS // don't transform, update checksums and timestamps
  = Value
}

/**
 * Requests the materialization of a view
 * @param mode @see MaterializeViewMode
 */
case class MaterializeView(mode: MaterializeViewMode.MaterializeViewMode = MaterializeViewMode.DEFAULT) extends CommandRequest

/**
 *  Command respnse
 */
sealed class CommandResponse
/**
 * Notifies of successful resource deployment by drivers. Sent to ?
 */
case class DeployActionSuccess() extends CommandResponse
/**
 * Successful Schema operation, sent by schemamanagerActor to Issuer of a SchemaCommand
 */
case class SchemaActionSuccess() extends CommandResponse
/**
 * Successful completion of a driver run
 * @constructor
 * @param driverRunHandle RunHandle of the executing driver
 * @param driverRunState return state of the driver
 */
case class ActionSuccess[T <: Transformation](driverRunHandle: DriverRunHandle[T], driverRunState: DriverRunSucceeded[T]) extends CommandResponse
/**
 * Contains state of queues
 * @param actionQueues List of queue states of type
 */
case class QueueStatusListResponse(val actionQueues: Map[String, List[AnyRef]]) extends CommandResponse
/**
 * Encapsulated list of action states
 * @param actionsStatusList List of entities of ActionStatusResponse
 * @see ActionStatusResponse
 */
case class ActionStatusListResponse(val actionStatusList: List[ActionStatusResponse[_]]) extends CommandResponse
/**
 * Encapsulated information about the state of a running DriverActor
 * @param message Textual description of state
 * @param message Reference to the driver actor
 * @param driverRunHandle runHandle of a running transformation
 * @param driverRunState state of a running transformation
 */
case class ActionStatusResponse[T <: Transformation](val message: String, val actor: ActorRef, val driver: Driver[T], driverRunHandle: DriverRunHandle[T], driverRunStatus: DriverRunState[T]) extends CommandResponse
/**
 * Encapsulates information of the state of a view
 * @param status textual description of the status
 * @param view reference to the curresponding view
 * @param actor actor reference to ViewActor
 */
case class ViewStatusResponse(val status: String, view: View, actor: ActorRef) extends CommandResponse
/**
 * Encapsulates List of Views
 * @param viewStatuslist list of view metadata
 * @see ViewStatusResponse
 */
case class ViewStatusListResponse(viewStatusList: List[ViewStatusResponse]) extends CommandResponse
/**
 * SchemaActor -> ViewActor, notification of View version match, response to GetVersion(view)
 */
case class ViewVersionOk(view: View) extends CommandResponse
/**
 * SchemaActor -> ViewActor, notification of View version match
 * @param view view in quesion
 */
case class ViewVersionMismatch(view: View, dataVersion: String) extends CommandResponse
/**
 * encapsulates the stored timestamp retrieved from metadata store, response to get
 * @param view view
 * @param timestamp time as epoch
 */
case class TransformationTimestamp(view: View, timestamp: Long) extends CommandResponse
/**
 * encapsulates transformation metadata (key value pairs) from the metadata store
 * @param  metadata contains metadata for a set of views
 */
case class TransformationMetadata(metadata: Map[View, (String, Long)]) extends CommandResponse
/**
 * notifies a depending view that one dependency has no data available
 */
case class NoDataAvailable(view: View) extends CommandResponse
/**
 * notification to views about a recomputation of a depenency
 * @param view View that has been changed
 * @param incomplete true of not all transitive dependencies had data available
 * @param transformationTimeStamp timestamp of the oldest? transformation in that dependency tree
 * @param errors true if some transformations in that subtree have been failing
 */
case class ViewMaterialized(view: View, incomplete: Boolean, transformationTimestamp: Long, errors: Boolean) extends CommandResponse
/**
 *
 */
