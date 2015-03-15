package com.ottogroup.bi.soda.bottler

import com.ottogroup.bi.soda.dsl.View
import java.util.Properties
import akka.actor.ActorRef
import org.joda.time.LocalDate
import org.joda.time.LocalDateTime
import com.ottogroup.bi.soda.bottler.driver.DriverRunState
import com.ottogroup.bi.soda.dsl.Transformation
import com.ottogroup.bi.soda.bottler.driver.DriverRunHandle
import com.ottogroup.bi.soda.bottler.driver.Driver
import com.ottogroup.bi.soda.bottler.driver.DriverRunSucceeded
import com.ottogroup.bi.soda.bottler.driver.DriverRunFailed
import com.ottogroup.bi.soda.bottler.driver.DriverException

class MessageType
class ErrorMessage extends MessageType

sealed class Success
case class ViewMaterialized(view: View, incomplete: Boolean, changed: Boolean, errors: Boolean) extends Success
case class NoDataAvailable(view: View) extends Success
case class ActionSuccess[T <: Transformation](driverRunHandle: DriverRunHandle[T], driverRunState: DriverRunSucceeded[T]) extends Success

sealed class Failure
case class Error(view: View, reason: String) extends Failure
case class FatalError(view: View, reason: String) extends Failure
case class ActorException(e: Throwable) extends Failure
case class Failed(view: View) extends Failure
case class InternalError(message: String) extends Failure
case class ActionFailure[T <: Transformation](driverRunHandle: DriverRunHandle[T], driverRunState: DriverRunFailed[T]) extends Failure
case class TimedOut() extends Failure

sealed class Command
case class NewDataAvailable(view: View) extends Command
case class GetStatus() extends Command
case class KillAction() extends Command
case class Suspend() extends Command
case class Deploy() extends Command
case class PollCommand(typ: String) extends Command
case class GetProcessList(sender: ActorRef, queues: Map[String, List[String]]) extends Command
case class CommandWithSender(command: AnyRef, sender: ActorRef) extends Command
case class CheckVersion(view: View) extends Command
case class SetVersion(view: View) extends Command

case class VersionOk(view: View)
case class VersionMismatch(view: View, dataVersion: String)
case class ViewStatusResponse(state: String, view: View)
case class ActionStatusResponse[T <: Transformation](message: String, actor: ActorRef, driver: Driver[T], driverRunHandle: DriverRunHandle[T], driverRunStatus: DriverRunState[T])
case class ProcessList(processStates: List[ActionStatusResponse[_]], queues: Map[String, List[String]])
