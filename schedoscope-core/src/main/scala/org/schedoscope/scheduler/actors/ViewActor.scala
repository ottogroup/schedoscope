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
package org.schedoscope.scheduler.actors

import java.security.PrivilegedAction
import akka.actor.ActorSelection.toScala
import akka.actor.{ Actor, ActorRef, Props, actorRef2Scala }
import akka.event.{ Logging, LoggingReceive }
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.{ Path, PathFilter, FileStatus }
import org.schedoscope.scheduler.driver.FileSystemDriver.defaultFileSystem
import org.schedoscope.{ AskPattern, SchedoscopeSettings }
import org.schedoscope.scheduler.states.{ ViewSchedulingState, ViewSchedulingAction }
import org.schedoscope.dsl.View
import org.schedoscope.dsl.transformations.{Touch, NoOp }
import org.schedoscope.scheduler.states.{ DependentView, AkkaActor, PartyInterestedInViewSchedulingStateChange, NoOpViewSchedulingStateMachineImpl, ViewSchedulingStateMachineImpl, Materialize, ReportMaterialized, ReportNotInvalidated, ReportInvalidated, ReportFailed, ReportNoDataAvailable, Transform, TouchSuccessFlag, WriteTransformationCheckum, WriteTransformationTimestamp, CreatedByViewManager, ReadFromSchemaManager, Invalidated, NoData, Failed, Waiting, Transforming, Materialized, Retrying }
import org.schedoscope.scheduler.messages.{ SetViewVersion, LogTransformationTimestamp, ViewStatusResponse, MaterializeView, ViewHasNoData, ViewFailed, ViewMaterialized, TransformationSuccess, TransformationFailure, Retry, InvalidateView }
import org.schedoscope.scheduler.states.AkkaActor

class ViewActor(var currentState: ViewSchedulingState, settings: SchedoscopeSettings, hdfs: FileSystem, viewManagerActor: ActorRef, transformationManagerActor: ActorRef, metadataLoggerActor: ActorRef) extends Actor {
  import AskPattern._
  import context._

  val log = Logging(system, this)

  def receive: Receive = LoggingReceive {
    {
      case MaterializeView(mode) =>
      case InvalidateView() =>
      case ViewHasNoData(dependency) =>
      case ViewFailed(dependency) =>
      case ViewMaterialized(dependency, incomplete, transformationTimestamp, withErrors) =>
      case _: TransformationSuccess[_] =>
      case _: TransformationFailure[_] =>
      case Retry() =>
    }
  }

  def performSchedulingActions(actions: Set[ViewSchedulingAction]) = actions
    .foreach {
      _ match {
        case WriteTransformationTimestamp(view, transformationTimestamp) => metadataLoggerActor ! LogTransformationTimestamp(view, transformationTimestamp)
        case WriteTransformationCheckum(view) => metadataLoggerActor ! SetViewVersion(view)
        case TouchSuccessFlag(view) => transformationManagerActor ! Touch(view.fullPath + "/_SUCCESS")
        case Materialize(view, mode) => actorForView(view) ! MaterializeView(mode)
        case Transform(view) => transformationManagerActor ! view
        case ReportNoDataAvailable(view, listeners) => listeners.foreach { actorForParty(_) ! ViewHasNoData(view) }
        case ReportFailed(view, listeners) => listeners.foreach { actorForParty(_) ! ViewFailed(view) }
        case ReportInvalidated(view, listeners) => listeners.foreach { actorForParty(_) ! ViewStatusResponse("invalidated", view, self) }
        case ReportNotInvalidated(view, listeners) =>
        case ReportMaterialized(view, listeners, transformationTimestamp, withErrors, incomplete) => listeners.foreach { actorForParty(_) ! ViewMaterialized(view, incomplete, transformationTimestamp, withErrors) }
      }
    }

  def stateMachine(view: View = currentState.view) = view.transformation() match {
    case NoOp() => new NoOpViewSchedulingStateMachineImpl(successFlagExists(view))
    case _      => new ViewSchedulingStateMachineImpl
  }

  def stateChange(currentState: ViewSchedulingState, updatedState: ViewSchedulingState) = currentState.getClass != updatedState.getClass

  def logStateChange(newState: ViewSchedulingState, previousState: ViewSchedulingState) {
    val stateHandle = newState match {
      case _: CreatedByViewManager | _: ReadFromSchemaManager => "receive"
      case _: Invalidated => "invalidated"
      case _: NoData => "nodata"
      case _: Waiting => "waiting"
      case _: Transforming => "transforming"
      case _: Materialized => "materialized"
      case _: Failed => "failed"
      case _: Retrying => "retrying"
    }

    viewManagerActor ! ViewStatusResponse(stateHandle, newState.view, self)

    log.info(s"VIEWACTOR STATE CHANGE ===> ${stateHandle.toUpperCase()}: newState${newState} previousState=$previousState} ")
  }

  
  
  def actorForParty(party: PartyInterestedInViewSchedulingStateChange) = party match {
    case DependentView(view) => actorForView(view)
    case AkkaActor(actor) => system.actorSelection(actor.path)
  }
  
  def actorForView(view: View) = ViewManagerActor.actorForView(view)
  
  def folderEmpty(view: View) = settings
    .userGroupInformation.doAs(
      new PrivilegedAction[Array[FileStatus]]() {
        def run() = {
          hdfs.listStatus(new Path(view.fullPath), new PathFilter() {
            def accept(p: Path): Boolean = !p.getName().startsWith("_")
          })
        }
      })
    .foldLeft(0l) {
      (size, status) => size + status.getLen()
    } > 0

  def successFlagExists(view: View) = settings
    .userGroupInformation.doAs(
      new PrivilegedAction[Boolean]() {
        def run() = {
          hdfs.exists(new Path(view.fullPath + "/_SUCCESS"))
        }
      })
}

object ViewActor {

  def props(state: ViewSchedulingState, settings: SchedoscopeSettings, hdfs: FileSystem, viewManagerActor: ActorRef, transformationManagerActor: ActorRef, metadataLoggerActor: ActorRef): Props = (Props(classOf[ViewActor], state, settings, hdfs, viewManagerActor, transformationManagerActor, metadataLoggerActor)).withDispatcher("akka.actor.views-dispatcher")

  def props(state: ViewSchedulingState, settings: SchedoscopeSettings, viewManagerActor: ActorRef, transformationManagerActor: ActorRef, metadataLoggerActor: ActorRef): Props = props(state, settings, defaultFileSystem(settings.hadoopConf), viewManagerActor, transformationManagerActor, metadataLoggerActor)

}
