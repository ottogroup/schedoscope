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
package org.schedoscope.scheduler

import scala.collection.mutable.HashMap
import org.schedoscope.SettingsImpl
import org.schedoscope.scheduler.RootActor.settings
import org.schedoscope.dsl.View
import org.schedoscope.scheduler.messages._
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.event.Logging
import akka.event.LoggingReceive
import org.schedoscope.dsl.ExternalTransformation
import scala.collection.mutable.HashSet
import kamon.Kamon

class ViewManagerActor(settings: SettingsImpl, actionsManagerActor: ActorRef, schemaActor: ActorRef, metadataLoggerActor: ActorRef) extends Actor {
  import context._
  val log = Logging(system, ViewManagerActor.this)

  val viewStatusMap = HashMap[String, ViewStatusResponse]()
  val viewCreationCounter = Kamon.metrics.counter("viewsCreated")
  override def preRestart(reason: Throwable, message: Option[Any]) {
    // prevent termination of children during restart and cause their own restart
  }

  def receive = LoggingReceive({
    case vsr: ViewStatusResponse => viewStatusMap.put(sender.path.toStringWithoutAddress, vsr)

    case GetViews(views, status, filter, dependencies) => {
      val viewActors = if (views.isDefined) initializeViewActors(views.get, dependencies) else List()
      val viewStates = viewStatusMap.values
        .filter(vs => !views.isDefined || viewActors.contains(vs.actor))
        .filter(vs => !status.isDefined || status.get.equals(vs.status))
        .filter(vs => !filter.isDefined || vs.view.urlPath.matches(filter.get))
        .toList

      sender ! ViewStatusListResponse(viewStates)
    }

    case NewDataAvailable(view) => children.filter { _ != sender }.foreach { _ ! NewDataAvailable(view) }

    case v: View => {
      sender ! initializeViewActors(List(v), false).headOption.getOrElse(List())
    }
  })

  def viewsToCreateActorsFor(views: List[View], dependencies: Boolean = false, depth: Int = 0, visited: HashSet[View] = HashSet()): List[(View, Boolean, Int)] = {
    views.map {
      v =>
        if (visited.contains(v))
          List()
        else if (ViewManagerActor.actorForView(v).isTerminated) {
          visited += v
          (v, true, depth) :: viewsToCreateActorsFor(v.dependencies.toList, dependencies, depth + 1, visited)
        } else if (dependencies) {
          visited += v
          (v, false, depth) :: viewsToCreateActorsFor(v.dependencies.toList, dependencies, depth + 1, visited)
        } else {
          visited += v
          List((v, false, depth))
        }

    }.flatten.distinct
  }

  def initializeViewActors(vs: List[View], dependencies: Boolean = false): List[ActorRef] = {
    log.info(s"Initializing ${vs.size} views")

    val allViews = viewsToCreateActorsFor(vs, dependencies)

    log.info(s"Computed ${allViews.size} views (with dependencies=${dependencies})")

    val actorsToCreate = allViews
      .filter { case (_, needsCreation, _) => needsCreation }

    log.info(s"Need to create ${actorsToCreate.size} actors")

    val viewsPerTableName = actorsToCreate
      .map { case (view, _, _) => view }
      .distinct
      .groupBy { _.tableName }
      .values
      .toList

    val tablesToCreate = viewsPerTableName
      .map { CheckOrCreateTables(_) }

    if (tablesToCreate.nonEmpty) {
      log.info(s"Submitting tables to check or create to schema actor")
      tablesToCreate.foreach {
        queryActor[Any](schemaActor, _, settings.schemaTimeout)
      }
    }

    val partitionsToCreate = viewsPerTableName
      .map { AddPartitions(_) }

    if (partitionsToCreate.nonEmpty) {
      log.info(s"Submitting ${partitionsToCreate.size} partition batches to schema actor")

      val viewsWithMetadataToCreate = queryActors[TransformationMetadata](schemaActor, partitionsToCreate, settings.schemaTimeout)

      log.info(s"Partitions created, initializing actors")

      viewsWithMetadataToCreate.foreach(
        _.metadata.foreach {
          case (view, (version, timestamp)) => {
            val actorRef = actorOf(ViewActor.props(view, settings, self, actionsManagerActor, metadataLoggerActor, version, timestamp), ViewManagerActor.actorNameForView(view))
            viewStatusMap.put(actorRef.path.toStringWithoutAddress, ViewStatusResponse("receive", view, actorRef))
          }
        })
    }

    if (dependencies)
      allViews.map { case (view, _, _) => ViewManagerActor.actorForView(view) }.distinct
    else
      allViews.filter { case (_, _, depth) => depth == 0 }.map { case (view, _, _) => ViewManagerActor.actorForView(view) }.distinct
  }
}

object ViewManagerActor {
  def props(settings: SettingsImpl, actionsManagerActor: ActorRef, schemaActor: ActorRef, metadataLoggerActor: ActorRef): Props = Props(classOf[ViewManagerActor], settings: SettingsImpl, actionsManagerActor, schemaActor, metadataLoggerActor).withDispatcher("akka.actor.view-manager-dispatcher")

  def actorNameForView(v: View) = v.urlPath.replaceAll("/", ":")

  def viewForActor(a: ActorRef) =
    View.viewsFromUrl(settings.env, a.path.name.replaceAll(":", "/"), settings.viewAugmentor).head

  def actorForView(v: View) = RootActor.settings.system.actorFor(RootActor.viewManagerActor.path.child(actorNameForView(v)))
}
