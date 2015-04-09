package com.ottogroup.bi.soda.bottler

import com.ottogroup.bi.soda.bottler.driver.Driver
import com.ottogroup.bi.soda.bottler.driver.DriverRunFailed
import com.ottogroup.bi.soda.bottler.driver.DriverRunHandle
import com.ottogroup.bi.soda.bottler.driver.DriverRunState
import com.ottogroup.bi.soda.bottler.driver.DriverRunSucceeded
import com.ottogroup.bi.soda.dsl.Transformation
import com.ottogroup.bi.soda.dsl.View

import akka.actor.ActorRef

sealed class Failure
case class Error(view: View, reason: String) extends Failure
case class Failed(view: View) extends Failure
case class ActionFailure[T <: Transformation](driverRunHandle: DriverRunHandle[T], driverRunState: DriverRunFailed[T]) extends Failure
case class SchemaActionFailure() extends Failure

sealed class CommandRequest
case class AddPartitions(views: List[View]) extends CommandRequest
case class CheckOrCreateTables(views: List[View]) extends CommandRequest
case class ViewList(views: List[View]) extends CommandRequest
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
case class GetStatus() extends CommandRequest
case class GetViewStatus(views: List[View], withDependencies: Boolean) extends CommandRequest
case class GetActionStatusList(statusRequester: ActorRef, actionQueueStatus: Map[String, List[String]], driverActors: Seq[ActorRef]) extends CommandRequest
case class GetViewStatusList(statusRequester: ActorRef, viewActors: Iterable[ActorRef]) extends CommandRequest
case class MaterializeView() extends CommandRequest

sealed class CommandResponse
case class DeployActionSuccess() extends CommandResponse
case class SchemaActionSuccess() extends CommandResponse
case class ActionSuccess[T <: Transformation](driverRunHandle: DriverRunHandle[T], driverRunState: DriverRunSucceeded[T]) extends CommandResponse
case class ActionStatusListResponse(val actionStatusList: List[ActionStatusResponse[_]], val actionQueueStatus: Map[String, List[AnyRef]]) extends CommandResponse
case class ActionStatusResponse[T <: Transformation](val message: String, val actor: ActorRef, val driver: Driver[T], driverRunHandle: DriverRunHandle[T], driverRunStatus: DriverRunState[T]) extends CommandResponse
case class ViewStatusResponse(val status: String, view: View) extends CommandResponse
case class ViewStatusListResponse(viewStatusList: List[ViewStatusResponse]) extends CommandResponse
case class ViewVersionOk(view: View) extends CommandResponse
case class ViewVersionMismatch(view: View, dataVersion: String) extends CommandResponse
case class TransformationTimestamp(view: View, timestamp: Long) extends CommandResponse
case class TransformationMetadata(metadata: Map[View, (String, Long)]) extends CommandResponse
case class NoDataAvailable(view: View) extends CommandResponse
case class ViewMaterialized(view: View, incomplete: Boolean, transformationTimestamp: Long, errors: Boolean) extends CommandResponse

