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
import org.schedoscope.conf.SchedoscopeSettings
import org.schedoscope.dsl.View
import org.schedoscope.scheduler.messages._
import org.schedoscope.scheduler.states.ViewSchedulingState

import scala.collection.mutable.HashMap

/**
  * The view manager actor is the factory and supervisor of table actors managing all views of a table. It also serves
  * as the central access point for the schedoscope service.
  *
  */
class ViewManagerActor(settings: SchedoscopeSettings,
                       actionsManagerActor: ActorRef,
                       schemaManagerRouter: ActorRef,
                       viewSchedulingListenerManagerActor: ActorRef
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
      viewStatusMap.put(vsr.view.urlPath, vsr)

    case GetViews(views, status, filter, issueFilter, withDependencies) =>
      val viewStates = viewStatusMap.values
        .filter(vs => views.isEmpty || tableActorsForViews(views.get.toSet, withDependencies).contains(vs.view))
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


    case v: View =>
      sender ! tableActorForView(v)


    case DelegateMessageToView(v, msg) =>
      val ref = tableActorForView(v)
      ref forward msg
      sender ! NewTableActorRef(v, ref)

  })

  /**
    * Convenience "private" method to validate external Views and their dependencies
    * prior to actor initialization
    */
  def validateExternalViews(vs: List[View]) {
    if (!settings.externalDependencies) {
      //external dependencies are not allowed
      val containsExternalDependencies = vs.exists {
        _.hasExternalDependencies
      }

      if (containsExternalDependencies)
        throw new UnsupportedOperationException("External dependencies are not enabled," +
          "if you are sure you wan't to use this feature enable it in the schedoscope.conf.")
    } else if (settings.externalChecksEnabled) {
      vs.foreach { v =>
        if (v.isInDatabases(settings.externalHome: _*)) {
          if (v.isExternal)
            throw new UnsupportedOperationException(s"You are referencing an external view as internal: $v.")
        } else {
          if (!v.isExternal)
            throw new UnsupportedOperationException(s"You are referencing an internal view as external: $v.")
        }
      }
    }
  }

  /**
    * This method returns the table actor for the given view, creating it if necessary.
    *
    * @param v the view to obtain table actor for
    * @return the actor
    */
  def tableActorForView(v: View): ActorRef = tableActorsForViews(Set(v), false).head._2

  /**
    * This method returns the table actors for the given views, creating them if necessary.
    *
    * @param vs               the views to obtain table actors for
    * @param withDependencies includes the dependencies of the given views (defaults to false)
    * @return a map assigning each view its responsible table actor.
    */
  def tableActorsForViews(vs: Set[View], withDependencies: Boolean = false): Map[View, ActorRef] = {

    val viewsRequiringInitialization = unknownViewsOrDependencies(vs.toList)

    validateExternalViews(viewsRequiringInitialization)

    val viewsPerTable = viewsRequiringInitialization
      .groupBy(_.urlPathPrefix)
      .map { case (_, views) => views }

    //
    // a) Create table actors for views requiring initialization if necessary
    // b) Send table actors initialize messages with those views
    // c) register views in view status map
    //
    viewsPerTable.foreach { vst =>

      val tableActorRef = existingTableActorForView(vst.head).getOrElse(
        actorOf(
          TableActor.props(
            Map.empty[View, ViewSchedulingState],
            settings,
            Map.empty[String, ActorRef],
            self,
            actionsManagerActor,
            schemaManagerRouter,
            viewSchedulingListenerManagerActor), tableActorNameForView(vst.head)
        )
      )

      tableActorRef ! InitializeViews(vst)

      vst.foreach(v => viewStatusMap.put(v.urlPath, ViewStatusResponse("receive", v, tableActorRef)))
    }

    //
    // Make table actors for dependencies known to table actors
    //
    viewsPerTable.flatten.foreach { v =>

      val newDepsActorRefs = v
        .dependencies
        .flatMap(v => existingTableActorForView(v).map(NewTableActorRef(v, _)))

      existingTableActorForView(v) match {
        case Some(actorRef) =>
          newDepsActorRefs.foreach(actorRef ! _)

        case None => //actor not yet known nothing to do here
      }
    }

    //
    // Finally return all views (initialized or already existing) that were addressed by the caller along with their table actors
    //
    val addressedViews =
    if (withDependencies)
      vs ++ vs.flatMap(_.transitiveDependencies)
    else
      vs

    addressedViews.map(v => v -> viewStatusMap(v.urlPath).actor).toMap
  }

  /**
    * This method returns for a given set of views along with their dependencies, which of those are yet known
    * to the view manager actor
    *
    * @param vs views to inspect along with their dependecies
    * @return the view needing initialization
    */
  def unknownViewsOrDependencies(vs: List[View], visited: Set[View] = Set()): List[View] =
    vs.flatMap { v =>
      if (visited.contains(v) || viewStatusMap.contains(v.urlPath))
        List()
      else
        v :: unknownViewsOrDependencies(v.dependencies, visited + v)
    }

  /**
    * Returns the responsible table actor for a view if it exists.
    */
  def existingTableActorForView(view: View): Option[ActorRef] =
    child(tableActorNameForView(view))

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

  def tableActorNameForView(view: View): String = view.urlPathPrefix.replaceAll("/", ":")
}
