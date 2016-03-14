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
import java.lang.Math.pow
import akka.actor.ActorSelection.toScala
import akka.actor.{ Actor, ActorRef, Props, actorRef2Scala }
import akka.event.{ Logging, LoggingReceive }
import org.apache.hadoop.fs.{ FileStatus, FileSystem, Path, PathFilter }
import org.schedoscope.SchedoscopeSettings
import org.schedoscope.dsl.View
import org.schedoscope.dsl.transformations.{ NoOp, Touch }
import org.schedoscope.scheduler.driver.FileSystemDriver.defaultFileSystem
import org.schedoscope.scheduler.messages.{ InvalidateView, LogTransformationTimestamp, MaterializeView, Retry, SetViewVersion, TransformationFailure, TransformationSuccess, ViewFailed, ViewHasNoData, ViewMaterialized, ViewStatusResponse }
import org.schedoscope.scheduler.states._
import scala.concurrent.duration.Duration

class ViewActor(var currentState: ViewSchedulingState, settings: SchedoscopeSettings, hdfs: FileSystem, viewManagerActor: ActorRef, transformationManagerActor: ActorRef, metadataLoggerActor: ActorRef) extends Actor {
  import context._

  val log = Logging(system, this)

  def receive: Receive = LoggingReceive {
    {

      case MaterializeView(mode) => stateTransition {
        stateMachine.materialize(currentState, sender, mode)
      }

      case InvalidateView() => stateTransition {
        stateMachine.invalidate(currentState, sender)
      }

      case ViewHasNoData(dependency) => stateTransition {
        stateMachine.noDataAvailable(currentState.asInstanceOf[Waiting], dependency)
      }

      case ViewFailed(dependency) => stateTransition {
        stateMachine.failed(currentState.asInstanceOf[Waiting], dependency)
      }

      case ViewMaterialized(dependency, incomplete, transformationTimestamp, withErrors) => stateTransition {
        stateMachine.materialized(currentState.asInstanceOf[Waiting], dependency, transformationTimestamp, withErrors, incomplete)
      }

      case _: TransformationSuccess[_] => stateTransition {
        stateMachine.transformationSucceeded(currentState.asInstanceOf[Transforming], folderEmpty(currentState.view))
      }

      case _: TransformationFailure[_] => stateTransition {
        val s = currentState.asInstanceOf[Transforming]
        val result = stateMachine.transformationFailed(s)
        val retry = s.retry

        result.currentState match {
          case r: Retrying =>
            system
              .scheduler
              .scheduleOnce(Duration.create(pow(2, retry).toLong, "seconds")) {
                self ! Retry()
              }

            result

          case _ => result
        }
      }

      case Retry() => stateTransition {
        stateMachine.retry(currentState.asInstanceOf[Retrying])
      }
    }
  }

  def stateTransition(messageApplication: ResultingViewSchedulingState) = messageApplication match {
    case ResultingViewSchedulingState(updatedState, actions) => {
      if (stateChange(currentState, updatedState))
        logStateChange(updatedState, currentState)

      performSchedulingActions(actions)

      currentState = updatedState
    }
  }

  def performSchedulingActions(actions: Set[ViewSchedulingAction]) = actions.foreach {

    case WriteTransformationTimestamp(view, transformationTimestamp) =>
      metadataLoggerActor ! LogTransformationTimestamp(view, transformationTimestamp)

    case WriteTransformationCheckum(view) =>
      metadataLoggerActor ! SetViewVersion(view)

    case TouchSuccessFlag(view) =>
      transformationManagerActor ! Touch(view.fullPath + "/_SUCCESS")

    case Materialize(view, mode) =>
      actorForView(view) ! MaterializeView(mode)

    case Transform(view) =>
      transformationManagerActor ! view

    case ReportNoDataAvailable(view, listeners) =>
      listeners.foreach {
        actorForParty(_) ! ViewHasNoData(view)
      }

    case ReportFailed(view, listeners) =>
      listeners.foreach {
        actorForParty(_) ! ViewFailed(view)
      }

    case ReportInvalidated(view, listeners) =>
      listeners.foreach {
        actorForParty(_) ! ViewStatusResponse("invalidated", view, self)
      }

    case ReportNotInvalidated(view, listeners) =>
      log.warning("VIEWACTOR: Could not invalidate view ${view}")

    case ReportMaterialized(view, listeners, transformationTimestamp, withErrors, incomplete) =>
      listeners.foreach {
        actorForParty(_) ! ViewMaterialized(view, incomplete, transformationTimestamp, withErrors)
      }
  }

  def stateMachine: ViewSchedulingStateMachine = stateMachine(currentState.view)

  def stateMachine(view: View): ViewSchedulingStateMachine = view.transformation() match {
    case _: NoOp => new NoOpViewSchedulingStateMachineImpl(successFlagExists(view))
    case _       => new ViewSchedulingStateMachineImpl
  }

  def stateChange(currentState: ViewSchedulingState, updatedState: ViewSchedulingState) = currentState.getClass != updatedState.getClass

  def logStateChange(newState: ViewSchedulingState, previousState: ViewSchedulingState) {
    viewManagerActor ! ViewStatusResponse(newState.label, newState.view, self)

    log.info(s"VIEWACTOR STATE CHANGE ===> ${newState.label.toUpperCase()}: newState=${newState} previousState=${previousState}")
  }

  def actorForParty(party: PartyInterestedInViewSchedulingStateChange) = party match {
    case DependentView(view) => actorForView(view)
    case AkkaActor(actor)    => system.actorSelection(actor.path)
  }

  def actorForView(view: View) = ViewManagerActor.actorForView(view)

  def folderEmpty(view: View) = settings
    .userGroupInformation.doAs(
      new PrivilegedAction[Array[FileStatus]]() {
        def run() = {
          hdfs.listStatus(new Path(view.fullPath), new PathFilter() {
            def accept(p: Path): Boolean = !p.getName.startsWith("_")
          })
        }
      })
    .foldLeft(0l) {
      (size, status) => size + status.getLen
    } <= 0

  def successFlagExists(view: View) = settings
    .userGroupInformation.doAs(
      new PrivilegedAction[Boolean]() {
        def run() = {
          hdfs.exists(new Path(view.fullPath + "/_SUCCESS"))
        }
      })
}

object ViewActor {

  def props(state: ViewSchedulingState, settings: SchedoscopeSettings, hdfs: FileSystem, viewManagerActor: ActorRef, transformationManagerActor: ActorRef, metadataLoggerActor: ActorRef): Props = Props(classOf[ViewActor], state, settings, hdfs, viewManagerActor, transformationManagerActor, metadataLoggerActor).withDispatcher("akka.actor.views-dispatcher")

  def props(state: ViewSchedulingState, settings: SchedoscopeSettings, viewManagerActor: ActorRef, transformationManagerActor: ActorRef, metadataLoggerActor: ActorRef): Props = props(state, settings, defaultFileSystem(settings.hadoopConf), viewManagerActor, transformationManagerActor, metadataLoggerActor)

}
