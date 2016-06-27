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

import akka.actor.ActorRef
import org.schedoscope.dsl.View
import org.schedoscope.dsl.transformations.Transformation
import org.schedoscope.scheduler.driver.{ Driver, DriverRunFailed, DriverRunHandle, DriverRunState, DriverRunSucceeded }

/**
 * Superclass for failure messages.
 */
sealed class Failure

/**
 * View actor signals its materialization failure to a waiting view actor
 */
case class ViewFailed(view: View) extends Failure

/**
 * Driver actor signaling a failure of a transfomation that requires a retry by the receiving view actor.
 */
case class TransformationFailure[T <: Transformation](driverRunHandle: DriverRunHandle[T], driverRunState: DriverRunFailed[T]) extends Failure

/**
 * Superclass for commands sent to actors.
 */
sealed class CommandRequest

/**
 * Instructs the partition creator actor to verify or create all Partitions that are specified in any
 * of the views in parameter 'views'
 */
case class AddPartitions(views: List[View]) extends CommandRequest

/**
 * Instructs the partition creator actor to verify or create all tables defined in the specified list of views
 */
case class CheckOrCreateTables(views: List[View]) extends CommandRequest

/**
 * Instructs the metadata logger actor to record the version information of the given view
 * in the metastore.
 */
case class SetViewVersion(view: View) extends CommandRequest

/**
 * Instructs the metadata logger actor to record the timestamp of the last transformation of a
 * given view in the metastore
 */
case class LogTransformationTimestamp(view: View, timestamp: Long) extends CommandRequest

/**
 * Command to kill a running transformation. Actual outcome depends on the ability of the
 * associated driver to be able to do that.
 */
case class KillCommand() extends CommandRequest

/**
 * Command to driver actors to instruct their drivers to deploy their resources (e.g. UDFS) to the distributed file system
 */
case class DeployCommand() extends CommandRequest

/**
 * Used by driver actors to poll the transformation manager actor for a new piece of work to be executed.
 */
case class PollCommand(typ: String) extends CommandRequest

/**
 * Instructs a driver actor to perform a command, such as a transformation. It comes along
 * with the reference to the actor that requested the action. The driver actor can then
 * notify the sender about the outcome.
 */
case class CommandWithSender(command: AnyRef, sender: ActorRef) extends CommandRequest

/**
 * Request to the transformation manager to generate a summary of currently running actions
 */
case class GetTransformations() extends CommandRequest

/**
 * Request to the tranformation manager to retrieve the contents and size of the transformation queues
 */
case class GetQueues() extends CommandRequest

/**
 * Request to the transformation manager to return the state of all driver actors
 */
case class GetTransformationStatusList(statusRequester: ActorRef, transformationQueueStatus: Map[String, List[String]], driverActors: Seq[ActorRef]) extends CommandRequest

/**
 * Request to the view manager actor to retrieve information of the currently instantiated views
 * @param views  A list of views to retrieve information from, may be empty
 * @param status filter the result by view status
 * @param filter filter the result by regular expression on the view name
 * @param dependencies also return all dependent views
 */
case class GetViews(views: Option[List[View]], status: Option[String], filter: Option[String], dependencies: Boolean = false)

/**
 * Request to the view manager to return the state of all views.
 */
case class GetViewStatusList(statusRequester: ActorRef, viewActors: Iterable[ActorRef]) extends CommandRequest

/**
 * Flags for the MaterializeView command
 */
object MaterializeViewMode extends Enumeration {
  type MaterializeViewMode = Value

  val DEFAULT, // no special mode
  RESET_TRANSFORMATION_CHECKSUMS, // do not consider version checksum changes when making transformation decisions
  RESET_TRANSFORMATION_CHECKSUMS_AND_TIMESTAMPS, // perform a transformation dry run, only update checksums and timestamps
  TRANSFORM_ONLY, // directly transform a view without materializing its dependencies
  SET_ONLY // set the view to materialized without materializing its dependencies and without transforming itself
  = Value
}

/**
 * Instructs a view actor to materialize itself
 */
case class MaterializeView(mode: MaterializeViewMode.MaterializeViewMode = MaterializeViewMode.DEFAULT) extends CommandRequest

/**
 * Instructs a view actor to assume that its data needs to be recomputed.
 */
case class InvalidateView() extends CommandRequest

/**
 * Instructs a view-actor to retry a transformation after a failure
 */
case class Retry() extends CommandRequest

/**
 * Base class for responses to commands.
 */
sealed class CommandResponse

/**
 * Driver actor notifying the transformation manager actor of successful resource deployment.
 */
case class DeployCommandSuccess() extends CommandResponse

/**
 * Schema actor or metadata logger notifying view manager actor or view actor of successful schema action.
 */
case class SchemaActionSuccess() extends CommandResponse

/**
 * Driver actor notifying view actor of successful transformation.
 *
 * @param driverRunHandle RunHandle of the executing driver
 * @param driverRunState return state of the driver
 */
case class TransformationSuccess[T <: Transformation](driverRunHandle: DriverRunHandle[T], driverRunState: DriverRunSucceeded[T]) extends CommandResponse

/**
 * Response message of transformation manager actor with state of queues
 * @param transformationQueues List of queue states of type
 */
case class QueueStatusListResponse(val transformationQueues: Map[String, List[AnyRef]]) extends CommandResponse

/**
 * Response message of transformation manager actor with state of actions
 *
 * @param actionsStatusList List of entities of TransformationStatusResponse
 * @see TransformationStatusResponse
 */
case class TransformationStatusListResponse(val transformationStatusList: List[TransformationStatusResponse[_]]) extends CommandResponse

/**
 * Response message of view manager actor with state of view actors
 *
 * @param viewStatusList list of view metadata
 * @see ViewStatusResponse
 */
case class ViewStatusListResponse(viewStatusList: List[ViewStatusResponse]) extends CommandResponse

/**
 * Driver actor responding to the transformation manager actor with the state of the running transformation
 *
 * @param message Textual description of state
 * @param actor Reference to the driver actor
 * @param driverRunHandle runHandle of a running transformation
 * @param driverRunStatus state of a running transformation
 */
case class TransformationStatusResponse[T <: Transformation](val message: String, val actor: ActorRef, val driver: Driver[T], driverRunHandle: DriverRunHandle[T], driverRunStatus: DriverRunState[T]) extends CommandResponse

/**
 * View actor responding to the view manager actor with the state of the view
 *
 * @param status textual description of the status
 * @param view reference to the curresponding view
 * @param actor actor reference to ViewActor
 */
case class ViewStatusResponse(val status: String, view: View, actor: ActorRef) extends CommandResponse

/**
 * Schema actor returning the stored transformation metadata (version checksum, timestamp) retrieved from metadata store
 * @param  metadata contains metadata for a set of views
 */
case class TransformationMetadata(metadata: Map[View, (String, Long)]) extends CommandResponse

/**
 * A view actor notifying a depending view that it has no data available
 */
case class ViewHasNoData(view: View) extends CommandResponse

/**
 * A view actor notifying a depending view that it has materialized
 *
 * @param view View that has been changed
 * @param incomplete true of not all transitive dependencies had data available
 * @param transformationTimestamp timestamp of the oldest? transformation in that dependency tree
 * @param errors true if some transformations in that subtree have been failing
 */
case class ViewMaterialized(view: View, incomplete: Boolean, transformationTimestamp: Long, errors: Boolean) extends CommandResponse
