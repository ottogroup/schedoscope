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
import org.schedoscope.scheduler.states.{CreatedByViewManager, ReadFromSchemaManager}

import scala.collection.mutable.{HashMap, HashSet}

/**
  * The view manager actor is the factory and import org.schedoscope.scheduler.actors.ViewActor
  * supervisor of view actors. Upon creation of view actors
  * it is responsible for creating non-existing tables or partitions in the Hive metastore, for reading
  * the last transformation timestamps and version checksums from the metastore for already materialized
  * views.
  *
  * It does this by cooperating with the partition creator actor and metadata logger actor.
  */
class ViewManagerActor(settings: SchedoscopeSettings, actionsManagerActor: ActorRef, schemaManagerRouter: ActorRef) extends Actor {

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
    case vsr: ViewStatusResponse => viewStatusMap.put(sender.path.toStringWithoutAddress, vsr)

    case GetViews(views, status, filter, issueFilter, dependencies) => {
      val viewActors: Set[ActorRef] = if (views.isDefined) initializeViewActors(views.get, dependencies) else Set()
      val viewStatesTest = viewStatusMap.values
      val viewStates = viewStatusMap.values
        .filter(vs => !views.isDefined || viewActors.contains(vs.actor))
        .filter(vs => !status.isDefined || status.get.equals(vs.status))
        .filter(vs => !filter.isDefined || vs.view.urlPath.matches(filter.get))
        .filter(vs => !issueFilter.isDefined
          || ("incomplete:" + vs.incomplete.getOrElse(false).toString).equals({
          val a = issueFilter.get.split("OR").filter(s => s.contains("incomplete"))
          if(a.size > 0) a.head else issueFilter.get})
          || ("errors:" + vs.errors.getOrElse(false).toString).equals({
          val a = issueFilter.get.split("OR").filter(s => s.contains("errors"))
          if(a.size > 0) a.head else issueFilter.get})
          || (issueFilter.get.contains("AND") &&
          (
            ("incomplete:" + vs.incomplete.getOrElse(false).toString + "AND" +
              "errors:" + vs.errors.getOrElse(false).toString).equals(issueFilter.get)
              || ("errors:" + vs.errors.getOrElse(false).toString + "AND" +
              "incomplete:" + vs.incomplete.getOrElse(false).toString
              ).equals(issueFilter.get)
            )
          )
        ).toList

      sender ! ViewStatusListResponse(viewStates)
    }

    case v: View => {
      sender ! initializeViewActors(List(v), false).head
    }

    case DelegateMessageToView(view, msg) => {
      val ref = actorForView(view) match {
        case Some(actorRef) =>
          actorRef
        case None =>
          initializeViewActors(List(view), false).head
      }
      ref forward msg
      sender ! NewViewActorRef(view, ref)
    }
  })


  /**
    * Initialize view actors for a list of views. If a view actor has been produced for a view
    * previously, that one is returned.
    *
    * @param vs           the views to create actors for
    * @param dependencies create actors for the prerequisite views as well.
    * @return the set of corresponding view actor refs
    */
  def initializeViewActors(vs: List[View], dependencies: Boolean = false):Set[ActorRef] = {
    log.info(s"Initializing ${vs.size} views")

    val allViews = viewsToCreateActorsFor(vs, dependencies)

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

    log.info(s"Computed ${allViews.size} views (with dependencies=${dependencies})")

    val actorsToCreate = allViews
      .filter { case (_, needsCreation, _) => needsCreation }

    log.info(s"Need to create ${actorsToCreate.size} actors")

    val viewsPerTableName = actorsToCreate
      .map { case (view, _, _) => view }
      .distinct
      .groupBy {
        _.tableName
      }
      .values
      .toList

    val tablesToCreate = viewsPerTableName
      .map {
        CheckOrCreateTables(_)
      }

    if (tablesToCreate.nonEmpty) {
      log.info(s"Submitting tables to check or create to schema actor")
      tablesToCreate.foreach {
        queryActor[Any](schemaManagerRouter, _, settings.schemaTimeout)
      }
    }

    val partitionsToCreate = viewsPerTableName
      .map {
        AddPartitions(_)
      }

    if (partitionsToCreate.nonEmpty) {
      log.info(s"Submitting ${partitionsToCreate.size} partition batches to schema actor")

      val viewsWithMetadataToCreate = queryActors[TransformationMetadata](schemaManagerRouter, partitionsToCreate, settings.schemaTimeout)

      log.info(s"Partitions created, initializing actors")

      viewsWithMetadataToCreate.foreach { t =>
        t.metadata.foreach {
          case (view, (version, timestamp)) => {

            val initialState = getStateFromMetadata(view, version, timestamp)

            val actorRef = actorOf(ViewActor.props(
              initialState,
              settings,
              Map.empty[View, ActorRef],
              self,
              actionsManagerActor,
              schemaManagerRouter), actorNameForView(view))
            viewStatusMap.put(actorRef.path.toStringWithoutAddress, ViewStatusResponse("receive", view, actorRef))
          }
        }

        log.info(s"Created actors for view table ${t.metadata.head._1.dbName}.${t.metadata.head._1.n}")
      }

      viewsWithMetadataToCreate.foreach { t =>
        t.metadata.foreach {
          case (view, _) =>
            val newDepsActorRefs = view
              .dependencies
              .flatMap(v => actorForView(v).map(NewViewActorRef(v, _)))

            actorForView(view) match {
              case Some(actorRef) =>
                newDepsActorRefs.foreach(actorRef ! _)
              case None => //actor not yet known nothing to do here
            }
        }
      }

    }
    log.info(s"Returning actors${if (dependencies) " including dependencies."}")

    val viewsToReturnActorRefsFor = if (dependencies)
      allViews.map { case (view, _, _) => view }.toSet
    else
      allViews.filter { case (_, _, depth) => depth == 0 }.map { case (view, _, _) => view }.toSet

    log.info(s"Fetching ${viewsToReturnActorRefsFor.size} actors")

    val actors = viewsToReturnActorRefsFor.map {
      view =>
        child(actorNameForView(view)).get
    }

    log.info(s"Returned ${actors.size} actors")

    actors
  }

  def viewsToCreateActorsFor(views: List[View], dependencies: Boolean = false, depth: Int = 0, visited: HashSet[View] = HashSet()): List[(View, Boolean, Int)] =
    views.flatMap {
      v =>
        if (visited.contains(v))
          List()
        else if (child(actorNameForView(v)).isEmpty) {
          visited += v
          (v, true, depth) :: viewsToCreateActorsFor(v.dependencies.toList, dependencies, depth + 1, visited)
        } else if (dependencies) {
          visited += v
          (v, false, depth) :: viewsToCreateActorsFor(v.dependencies.toList, dependencies, depth + 1, visited)
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
            schemaManagerRouter: ActorRef): Props = Props(classOf[ViewManagerActor], settings: SchedoscopeSettings, actionsManagerActor, schemaManagerRouter).withDispatcher("akka.actor.view-manager-dispatcher")

  /**
    * Helper to convert state to MetaData
    *
    * @param view
    * @param version
    * @param timestamp
    * @return current [[org.schedoscope.scheduler.states.ViewSchedulingState]] of the view
    */
  def getStateFromMetadata(view: View, version: String, timestamp: Long) = {
    if ((version != Checksum.defaultDigest) || (timestamp > 0))
      ReadFromSchemaManager(view, version, timestamp)
    else
      CreatedByViewManager(view)
  }

  def actorNameForView(view: View) = view.urlPath.replaceAll("/", ":")
}
