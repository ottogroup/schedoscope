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

import java.lang.Math.pow
import java.security.PrivilegedAction

import akka.actor.ActorSelection.toScala
import akka.actor.{Actor, ActorRef, Props, actorRef2Scala}
import akka.event.{Logging, LoggingReceive}
import akka.pattern.ask
import akka.util.Timeout
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.schedoscope.AskPattern.queryActor
import org.schedoscope.conf.SchedoscopeSettings
import org.schedoscope.dsl.View
import org.schedoscope.dsl.transformations.{NoOp, Touch}
import org.schedoscope.scheduler.driver.FilesystemDriver.defaultFileSystem
import org.schedoscope.scheduler.messages._
import org.schedoscope.scheduler.states._

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, _}
import scala.util.{Failure, Success, Try}


class ViewActor(var currentState: ViewSchedulingState,
                settings: SchedoscopeSettings,
                hdfs: FileSystem,
                viewManagerActor: ActorRef,
                transformationManagerActor: ActorRef,
                metadataLoggerActor: ActorRef,
                partitionCreatorActor: ActorRef) extends Actor {

  import context._

  lazy val stateMachine: ViewSchedulingStateMachine = stateMachine(currentState.view)

  val log = Logging(system, this)
  var knownDependencies = currentState.view.dependencies.toSet

  def receive: Receive = LoggingReceive {
    {

      case MaterializeView(mode) => stateTransition {
        val currentView = currentState.view
        if (!currentView.isExternal) {
          stateMachine.materialize(currentState, sender, mode)
        } else {
          implicit val timeout = Timeout(5 seconds)
          //update state if external and NoOp view
          val future = ask(partitionCreatorActor, AddPartitions(List(currentView))).mapTo[TransformationMetadata]
          val result = Try(Await.result(future, timeout.duration))

          val currentExternalState = result match {
            case Success(TransformationMetadata(map)) =>
              map.head match {
                case (view, (version, timestamp)) =>
                  ViewManagerActor.getStateFromMetadata(view, version, timestamp)
              }
            case Failure(t) =>
              log.warning("VIEWACTOR: Could not get state of external view ${view}", t)
              currentState
          }
          stateMachine.materialize(currentExternalState, sender, mode)
        }
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

      val previousState = currentState

      currentState = updatedState
      performSchedulingActions(actions)

      if (stateChange(previousState, updatedState))
        logStateChange(updatedState, previousState)

    }
  }

  def performSchedulingActions(actions: Set[ViewSchedulingAction]) = actions.foreach {

    case WriteTransformationTimestamp(view, transformationTimestamp) =>
      if (!view.isExternal) metadataLoggerActor ! LogTransformationTimestamp(view, transformationTimestamp)


    case WriteTransformationCheckum(view) =>
      if (!view.isExternal) metadataLoggerActor ! SetViewVersion(view)


    case TouchSuccessFlag(view) =>
      touchSuccessFlag(view)

    case Materialize(view, mode) =>
      actorForView(view) ! MaterializeView(mode)

    case Transform(view) =>
      transformationManagerActor ! view

    case ReportNoDataAvailable(view, listeners) =>
      listeners.foreach {
        _ ! ViewHasNoData(view)
      }

    case ReportFailed(view, listeners) =>
      listeners.foreach {
        _ ! ViewFailed(view)
      }

    case ReportInvalidated(view, listeners) =>
      listeners.foreach {
        _ ! ViewStatusResponse("invalidated", view, self)
      }

    case ReportNotInvalidated(view, listeners) =>
      log.warning("VIEWACTOR: Could not invalidate view ${view}")

    case ReportMaterialized(view, listeners, transformationTimestamp, withErrors, incomplete) =>
      listeners.foreach {
        _ ! ViewMaterialized(view, incomplete, transformationTimestamp, withErrors)
      }
  }

  def actorForView(view: View) = {

    //
    // New dependencies may appear at a start of a new day. These might not yet have corresponding
    // actors created by the ViewManagerActor. Take care that a view actor is available before
    // returning an actor selection for it, as messages might end up in nirvana otherwise.
    //

    if (!knownDependencies.contains(view)) {
      queryActor[Any](viewManagerActor, view, settings.schedulingCommandTimeout)
      knownDependencies += view
    }

    ViewManagerActor.actorForView(view)
  }

  def touchSuccessFlag(view: View) {
    actorOf(Props(new Actor {
      override def preStart() {
        transformationManagerActor ! Touch(view.fullPath + "/_SUCCESS")
      }

      def receive = {
        case _ => context stop self
      }
    }))
  }

  def stateChange(currentState: ViewSchedulingState, updatedState: ViewSchedulingState) = currentState.getClass != updatedState.getClass

  def logStateChange(newState: ViewSchedulingState, previousState: ViewSchedulingState) {
    viewManagerActor ! ViewStatusResponse(newState.label, newState.view, self)

    log.info(s"VIEWACTOR STATE CHANGE ===> ${newState.label.toUpperCase()}: newState=${newState} previousState=${previousState}")
  }

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

  def stateMachine(view: View): ViewSchedulingStateMachine = view.transformation() match {
    case _: NoOp => new NoOpViewSchedulingStateMachineImpl(() => successFlagExists(view))
    case _ => new ViewSchedulingStateMachineImpl
  }

  def successFlagExists(view: View) = settings
    .userGroupInformation.doAs(
    new PrivilegedAction[Boolean]() {
      def run() = {
        hdfs.exists(new Path(view.fullPath + "/_SUCCESS"))
      }
    })
}

object ViewActor {
  def props(state: ViewSchedulingState,
            settings: SchedoscopeSettings,
            viewManagerActor: ActorRef,
            transformationManagerActor: ActorRef,
            metadataLoggerActor: ActorRef,
            partitionCreatorActor: ActorRef): Props =
    props(state,
      settings,
      defaultFileSystem(settings.hadoopConf),
      viewManagerActor,
      transformationManagerActor,
      metadataLoggerActor,
      partitionCreatorActor)

  def props(state: ViewSchedulingState,
            settings: SchedoscopeSettings,
            hdfs: FileSystem,
            viewManagerActor: ActorRef,
            transformationManagerActor: ActorRef,
            metadataLoggerActor: ActorRef,
            partitionCreatorActor: ActorRef): Props =
    Props(classOf[ViewActor],
      state,
      settings,
      hdfs,
      viewManagerActor,
      transformationManagerActor,
      metadataLoggerActor).withDispatcher("akka.actor.views-dispatcher")

}
