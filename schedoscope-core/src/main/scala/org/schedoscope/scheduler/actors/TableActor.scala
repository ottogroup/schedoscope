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

import akka.actor.{Actor, ActorRef, Props, actorRef2Scala}
import akka.event.{Logging, LoggingReceive}
import org.joda.time.LocalDateTime
import org.schedoscope.AskPattern.{queryActor, retryOnTimeout}
import org.schedoscope.conf.SchedoscopeSettings
import org.schedoscope.dsl.View
import org.schedoscope.dsl.transformations._
import org.schedoscope.scheduler.messages._
import org.schedoscope.scheduler.states._

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.language.implicitConversions

/**
  * Table actors manage the scheduling states of the views belonging to a given table.
  */
class TableActor(currentStates: Map[View, ViewSchedulingState],
                 settings: SchedoscopeSettings,
                 dependencies: Map[String, ActorRef],
                 viewManagerActor: ActorRef,
                 transformationManagerActor: ActorRef,
                 schemaManagerRouter: ActorRef,
                 viewSchedulingListenerManagerActor: ActorRef
                ) extends Actor {

  import context._

  val stateMachine: ViewSchedulingStateMachine = new ViewSchedulingStateMachineImpl

  val log = Logging(system, this)
  var knownDependencies = dependencies
  val viewStates = mutable.HashMap(currentStates.map {
    case (view, state) => view.urlPath -> state
  }.toSeq: _*)


  def receive: Receive = LoggingReceive {

    case CommandForView(sourceView, targetView, command) => {

      //mark the currentState as implicit for calling  stateTransition
      implicit val currentState = viewStates.get(targetView.urlPath) match {
        case Some(state) => state
        case None =>
          initializeViews(List(targetView)).head._2
      }

      val senderRef = AkkaActor(sourceView, sender)

      val currentView = currentState.view
      command match {

        case MaterializeView(mode) => stateTransition {
          stateMachine.materialize(currentState, senderRef, mode)
        }

        case MaterializeExternalView(mode) => {
          schemaManagerRouter ! GetMetaDataForMaterialize(currentView, mode, senderRef)
        }

        case MaterializeViewAsStub() => stateTransition {

          val sourcePath = s"hdfs://${settings.prodNameNode}/" +
            s"${currentView.fullPathBuilder(settings.prodEnv, settings.prodViewDataHdfsRoot)}"

          log.info(s"source path: $sourcePath")

          if (settings.devSshEnabled) {
            log.info(s"using ssh to execute distcp from ${settings.devSshTarget}")
            currentView.registeredTransformation = () => SshDistcpTransformation
              .copyFromProd(sourcePath, currentView, settings.devSshTarget)
          } else {
            currentView.registeredTransformation = () => DistCpTransformation.copyToDirToView(sourcePath, currentView)
          }

          //use transform only mode to mute dependencies
          stateMachine.materialize(currentState, senderRef, MaterializeViewMode.TRANSFORM_ONLY)
        }

        case MetaDataForMaterialize(metadata, mode, source) => stateTransition {
          val externalState = metadata match {
            case (view, (version, timestamp)) =>
              TableActor.stateFromMetadata(view, view.transformation().checksum, timestamp)
          }

          externalState match {
            case CreatedByViewManager(v) =>
              ResultingViewSchedulingState(
                NoData(v),
                Set(ReportNoDataAvailable(v, Set(source))))

            case _ => stateMachine.materialize(externalState, source, mode)
          }
        }

        case InvalidateView() => stateTransition {
          stateMachine.invalidate(currentState, senderRef)
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

        case s: TransformationSuccess[_] => stateTransition {
          stateMachine.transformationSucceeded(currentState.asInstanceOf[Transforming], !s.viewHasData)
        }

        case _: TransformationFailure[_] => stateTransition {
          val s = currentState.asInstanceOf[Transforming]
          val result = stateMachine.transformationFailed(s)
          val retry = s.retry

          result.currentState match {
            case _: Retrying =>
              system
                .scheduler
                .scheduleOnce(Duration.create(pow(2, retry).toLong, "seconds")) {
                  self ! CommandForView(None, targetView, Retry())
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

    case NewTableActorRef(view: View, viewRef: ActorRef) => {
      knownDependencies += view.tableName -> viewRef
    }

    case InitializeViews(views) => {
      initializeViews(views)
    }

    case SchemaActionSuccess() => {
      //do nothing
    }

    case other =>
      log.error(s"Illegal Message received by TableActor: $other")
      throw new IllegalArgumentException(s"Illegal Message received by TableActor")

  }


  def stateTransition(messageApplication: ResultingViewSchedulingState)
                     (implicit currentState: ViewSchedulingState) = messageApplication match {
    case ResultingViewSchedulingState(updatedState, actions) => {

      val currentView = currentState.view
      val previousState = currentState

      viewStates.put(currentView.urlPath, updatedState)
      performSchedulingActions(actions)

      notifySchedulingListeners(previousState, updatedState, actions)

      if (stateChange(previousState, updatedState))
        communicateStateChange(updatedState, previousState)

    }
  }

  def notifySchedulingListeners(previousState: ViewSchedulingState,
                                newState: ViewSchedulingState,
                                actions: scala.collection.immutable.Set[ViewSchedulingAction]) =
    if (!previousState.view.isExternal) {
      viewManagerActor ! ViewSchedulingMonitoringEvent(previousState, newState,
        actions, new LocalDateTime())
      viewSchedulingListenerManagerActor ! ViewSchedulingMonitoringEvent(previousState, newState,
        actions, new LocalDateTime())

    }

  def performSchedulingActions(actions: Set[ViewSchedulingAction])
                              (implicit currentState: ViewSchedulingState) = {

    implicit val reportingView = currentState.view

    actions.foreach {

      case WriteTransformationTimestamp(view, transformationTimestamp) =>
        if (!view.isExternal) schemaManagerRouter ! LogTransformationTimestamp(view, transformationTimestamp)

      case WriteTransformationCheckum(view) =>
        if (!view.isExternal) schemaManagerRouter ! SetViewVersion(view)

      case TouchSuccessFlag(view) =>
        touchSuccessFlag(view)

      case Materialize(view, mode) =>
        if (view.isExternal) {
          if (settings.developmentModeEnabled) {
            log.info(s"Materialize view as stub $view")
            sendMessageToView(view, MaterializeViewAsStub())
          } else {
            sendMessageToView(view, MaterializeExternalView(mode))
          }
        } else if (settings.developmentModeEnabled &&
          currentState.view.urlPathPrefix == settings.viewUnderDevelopment) {
          log.info(s"View is in development $view")
          //stub the dependent view
          sendMessageToView(view, MaterializeViewAsStub())
        } else {
          sendMessageToView(view, MaterializeView(mode))
        }

      case Transform(view) =>
        transformationManagerActor ! view

      case ReportNoDataAvailable(view, listeners) =>
        listeners.foreach {
          sendMessageToListener(_, ViewHasNoData(view))
        }

      case ReportFailed(view, listeners) =>
        listeners.foreach {
          sendMessageToListener(_, ViewFailed(view))
        }

      case ReportInvalidated(view, listeners) =>
        listeners.foreach {
          sendMessageToListener(_, ViewStatusResponse("invalidated", view, self))
        }

      case ReportNotInvalidated(view, listeners) =>
        log.warning(s"VIEWACTOR: Could not invalidate view ${view}")

      case ReportMaterialized(view, listeners, transformationTimestamp, withErrors, incomplete) =>
        listeners.foreach { l =>
          sendMessageToListener(l, ViewMaterialized(view, incomplete, transformationTimestamp, withErrors))
        }
    }
  }

  def sendMessageToListener(listener: PartyInterestedInViewSchedulingStateChange, msg: AnyRef)
                           (implicit reportingView: View): Unit =
    listener match {
      case DependentView(view) => sendMessageToView(view, msg)

      case AkkaActor(Some(view), actorRef) => actorRef ! CommandForView(Some(reportingView), view, msg)

      case AkkaActor(None, actorRef) => actorRef ! msg

      case _ => //Not supported
    }

  def sendMessageToView(view: View, msg: AnyRef)(implicit reportingView: View) = {

    //Package Message:
    val messageForView = CommandForView(Some(reportingView), view, msg)

    //
    // New dependencies may appear at a start of a new day. These might not yet have corresponding
    // actors created by the ViewManagerActor. Take care that a view actor is available before
    // returning an actor selection for it, as messages might end up in nirvana otherwise.
    //
    knownDependencies.get(view.tableName) match {
      case Some(r: ActorRef) =>
        r ! messageForView
      case None =>
        viewManagerActor ! DelegateMessageToView(view, messageForView)
    }

    //    ViewManagerActor.actorForView(view)
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

  def communicateStateChange(newState: ViewSchedulingState, previousState: ViewSchedulingState) {
    val vsr = newState match {
      case Waiting(view, _, _, _, _, _, _, withErrors, incomplete, _) =>
        ViewStatusResponse(newState.label, view, self, Some(withErrors), Some(incomplete))
      case Transforming(view, _, _, _, withErrors, incomplete, _) =>
        ViewStatusResponse(newState.label, view, self, Some(withErrors), Some(incomplete))
      case Materialized(view, _, _, withErrors, incomplete) =>
        ViewStatusResponse(newState.label, view, self, Some(withErrors), Some(incomplete))
      case Retrying(view, _, _, _, withErrors, incomplete, _) =>
        ViewStatusResponse(newState.label, view, self, Some(withErrors), Some(incomplete))
      case _ => ViewStatusResponse(newState.label, newState.view, self)
    }

    viewManagerActor ! vsr
  }

  def initializeViews(views: List[View]): Map[View, ViewSchedulingState] = {

    //Check which views have to be initialized
    val viewsToCreate = views.filterNot(v => viewStates.contains(v.urlPath))

    if (viewsToCreate.nonEmpty) {

      log.info(s"Creating table and / or partitions for ${viewsToCreate.head.dbName}.${viewsToCreate.head.n}")

      //If no views have been initialized the table schema should be created.
      if (viewStates.isEmpty) {

        log.info(s"Creating table if necessary for ${viewsToCreate.head.dbName}.${viewsToCreate.head.n}")

        retryOnTimeout(() =>
          queryActor[Any](schemaManagerRouter, CheckOrCreateTables(viewsToCreate), settings.schemaTimeout)
        )

      }

      log.info(s"Creating / reading partitions for $viewsToCreate")


      //Add the partitions
      val viewsWithMetadataToCreate = retryOnTimeout(() =>
        queryActor[TransformationMetadata](schemaManagerRouter, AddPartitions(viewsToCreate), settings.schemaTimeout)
      )

      log.info(s"Partitions created, initializing views")

      val newViews = viewsWithMetadataToCreate.metadata.map {

        case (view, (version, timestamp)) => {

          val initialState = TableActor.stateFromMetadata(view, if (view.isExternal) view.transformation().checksum else version, timestamp)
          viewStates.put(view.urlPath, initialState)
          //          sender ! ViewStatusResponse("receive", view, self)
          (view, initialState)
        }
      }

      log.info(s"Initialized table actor for views representing table ${viewsToCreate.head.dbName}.${viewsToCreate.head.n}")

      newViews
    } else {
      Map.empty[View, ViewSchedulingState]
    }
  }

}

object TableActor {
  def props(states: Map[View, ViewSchedulingState],
            settings: SchedoscopeSettings,
            dependencies: Map[String, ActorRef],
            viewManagerActor: ActorRef,
            transformationManagerActor: ActorRef,
            schemaManagerRouter: ActorRef,
            viewSchedulingListenerManagerActor: ActorRef): Props =
    Props(classOf[TableActor],
      states: Map[View, ViewSchedulingState],
      settings,
      dependencies,
      viewManagerActor,
      transformationManagerActor,
      schemaManagerRouter,
      viewSchedulingListenerManagerActor).withDispatcher("akka.actor.views-dispatcher")

  /**
    * Helper to convert partition / table metadata to view scheduling state
    *
    * @param view
    * @param version
    * @param timestamp
    * @return current [[org.schedoscope.scheduler.states.ViewSchedulingState]] of the view
    */
  def stateFromMetadata(view: View, version: String, timestamp: Long) = {
    if ((version != Checksum.defaultDigest) || (timestamp > 0))
      ReadFromSchemaManager(view, version, timestamp)
    else
      CreatedByViewManager(view)
  }

}

