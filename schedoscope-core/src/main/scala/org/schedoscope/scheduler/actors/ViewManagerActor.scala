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

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props, actorRef2Scala}
import akka.event.{Logging, LoggingReceive}
import org.schedoscope.AskPattern._
import org.schedoscope.conf.SchedoscopeSettings
import org.schedoscope.dsl.View
import org.schedoscope.dsl.transformations.Checksum
import org.schedoscope.scheduler.messages._
import org.schedoscope.scheduler.states.{CreatedByViewManager, ReadFromSchemaManager, ViewSchedulingState}

import scala.collection.mutable.{HashMap, HashSet}

/**
  * The view manager actor is the factory and supervisor of table actors managing all views of a table. It also serves
  * as the central access point for the schedoscope service.
  *
  */
class ViewManagerActor(settings: SchedoscopeSettings,
                       actionsManagerActor: ActorRef,
                       schemaManagerRouter: ActorRef,
                       viewSchedulingListenerManagerActor: ActorRef
                       //                       createChildActor: String => ActorRef
                      ) extends Actor {

  import ViewManagerActor._
  import context._

  /**
    * Supervisor strategy: Escalate any problems because view actor failures are not recoverable.
    */
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = -1) {
      case _ => Escalate
    }
  val log = Logging(system, ViewManagerActor.this)
  val viewStatusMap = HashMap[String, ViewStatusResponse]()

  /**
    * Message handler.
    */
  def receive = LoggingReceive({
    case vsr: ViewStatusResponse =>
      viewStatusMap.put(vsr.view.fullPath, vsr)

    //TODO: Review this part!
    case GetViews(views, status, filter, issueFilter, dependencies) => {
      val tableActorsForViews: Map[View, ActorRef] = if (views.isDefined) initializeViews(views.get, dependencies) else Map()

      val viewStates = viewStatusMap.values
        .filter(vs => views.isEmpty || tableActorsForViews.contains(vs.view))
        .filter(vs => status.isEmpty || status.get.equals(vs.status))
        .filter(vs => filter.isEmpty || vs.view.urlPath.matches(filter.get))
        .filter(vs => issueFilter.isEmpty || (
          List("materialized", "failed").contains(vs.status) && (
            ("incomplete".equals(issueFilter.get) && vs.incomplete.getOrElse(false))
              || ("errors".equals(issueFilter.get) && vs.errors.getOrElse(false))
              || (issueFilter.get.contains("AND") && vs.incomplete.getOrElse(false) && vs.errors.getOrElse(false))
            )
          )
        ).toList.distinct

      sender ! ViewStatusListResponse(viewStates)
    }

    case v: View => {
      sender ! initializeViews(List(v), includeExistingActors = false).head._2
    }

    case DelegateMessageToView(view, msg) => {
      val ref = actorForView(view) match {
        case Some(actorRef) =>
          actorRef
        case None =>
          initializeViews(List(view), false).head._2
      }
      ref forward msg
      sender ! NewTableActorRef(view, ref)
    }
  })

  /**
    * Convenience "private" method to validate external Views and their dependencies
    * prior to actor initialization
    */
  def validateExternalViews(allViews: List[(View, Boolean, Int)]): Unit =
    if (!settings.externalDependencies) {
      //external dependencies are not allowed
      val containsExternalDependencies = allViews.exists {
        case (view, _, _) => view.hasExternalDependencies
      }

      if (containsExternalDependencies)
        throw new UnsupportedOperationException("External dependencies are not enabled," +
          "if you are sure you wan't to use this feature enable it in the schedoscope.conf.")
    } else if (settings.externalChecksEnabled) {
      allViews.foreach { case (view, _, _) =>
        if (view.isInDatabases(settings.externalHome: _*)) {
          if (view.isExternal)
            throw new UnsupportedOperationException(s"You are referencing an external view as internal: $view.")
        } else {
          if (!view.isExternal)
            throw new UnsupportedOperationException(s"You are referencing an internal view as external: $view.")
        }
      }
    }

  def initializeViews(vs: List[View], includeExistingActors: Boolean = false): Map[View, ActorRef] = {

    val dependentViews = viewsToCreateActorsFor(vs, includeExistingActors)

    validateExternalViews(dependentViews)

    val viewsPerTableName = dependentViews
      .map { case (view, _, _) => view }
      .groupBy(_.urlPathPrefix)

    //    viewStatusMap.put(actorRef.path.toStringWithoutAddress, ViewStatusResponse("receive", view, actorRef))

    //sendViewsToTableActors / createMissingTableActor
    val actors : Map[View, ActorRef] = viewsPerTableName.flatMap {
      case (table, views) =>
        val actorRef = actorForView(views.head) match {
          case Some(tableActorRef) =>
            tableActorRef ! InitializeViews(views)
            tableActorRef
          case None =>
            // create actor
            val actorRef = actorOf(TableActor.props(
              Map.empty[View, ViewSchedulingState],
              settings,
              Map.empty[String, ActorRef],
              self,
              actionsManagerActor,
              schemaManagerRouter,
              viewSchedulingListenerManagerActor), actorNameForView(views.head))
            // send to actor
            actorRef ! InitializeViews(views)
            actorRef
        }
        views.map { v =>
          if (!viewStatusMap.contains(v.fullPath)) {
            viewStatusMap.put(v.fullPath, ViewStatusResponse("receive", v, actorRef))
          }
          v -> actorRef
        }
    }


    //TODO: Think about this!
    viewsPerTableName.flatMap(_._2).foreach { view =>
      val newDepsActorRefs = view
        .dependencies
        .flatMap(v => actorForView(v).map(NewTableActorRef(v, _)))

      actorForView(view) match {
        case Some(actorRef) =>
          newDepsActorRefs.foreach(actorRef ! _)
        case None => //actor not yet known nothing to do here
      }
    }

    actors
  }

  //TODO: review this method
  def viewsToCreateActorsFor(views: List[View], includeExistingActors: Boolean = false, depth: Int = 0, visited: HashSet[View] = HashSet()): List[(View, Boolean, Int)] =
    views.flatMap {
      v =>
        if (visited.contains(v))
          List()
        else if (child(actorNameForView(v)).isEmpty) {
          visited += v
          (v, true, depth) :: viewsToCreateActorsFor(v.dependencies.toList, includeExistingActors, depth + 1, visited)
        } else if (includeExistingActors) {
          visited += v
          (v, false, depth) :: viewsToCreateActorsFor(v.dependencies.toList, includeExistingActors, depth + 1, visited)
        } else {
          visited += v
          List((v, false, depth))
        }

    }.distinct

  def actorForView(view: View) =
    child(actorNameForView(view))

}

/**
  * View manager factory methods
  */
object ViewManagerActor {
  def props(settings: SchedoscopeSettings,
            actionsManagerActor: ActorRef,
            schemaManagerRouter: ActorRef,
            viewSchedulingListenerManagerActor: ActorRef): Props =
    Props(classOf[ViewManagerActor], settings: SchedoscopeSettings,
      actionsManagerActor, schemaManagerRouter, viewSchedulingListenerManagerActor)
      .withDispatcher("akka.actor.view-manager-dispatcher")

  def actorNameForView(view: View) = view.urlPathPrefix.replaceAll("/", ":")
}
