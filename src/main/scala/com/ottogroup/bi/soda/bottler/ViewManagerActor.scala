package com.ottogroup.bi.soda.bottler

import scala.collection.mutable.HashMap

import com.ottogroup.bi.soda.SettingsImpl
import com.ottogroup.bi.soda.bottler.SodaRootActor.settings
import com.ottogroup.bi.soda.dsl.View

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.event.Logging
import akka.event.LoggingReceive

class ViewManagerActor(settings: SettingsImpl, actionsManagerActor: ActorRef, schemaActor: ActorRef) extends Actor {
  import context._
  val log = Logging(system, ViewManagerActor.this)

  val viewStatusMap = HashMap[String, ViewStatusResponse]()

  override def preRestart(reason: Throwable, message: Option[Any]) {
    // prevent termination of children during restart and cause their own restart
  }

  def receive = LoggingReceive({
    case vsr: ViewStatusResponse => viewStatusMap.put(sender.path.toStringWithoutAddress, vsr)

    case GetStatus() => sender ! ViewStatusListResponse(viewStatusMap.values.toList)

    case GetViewStatus(views, withDependencies) => {
      val actorPaths = initializeViewActors(views, withDependencies).map(a => a.path.toStringWithoutAddress).toSet

      sender ! ViewStatusListResponse(viewStatusMap.filter { case (actorPath, _) => actorPaths.contains(actorPath) }.values.toList)
    }

    case NewDataAvailable(view) => children.filter { _ != sender }.foreach { _ ! NewDataAvailable(view) }

    case ViewList(views) => {
      sender ! initializeViewActors(views, false)
    }

    case v: View => {
      sender ! initializeViewActors(List(v), false).headOption.getOrElse(List())
    }
  })

  def actorsForViews(views: List[View], withDependencies: Boolean = false, depth: Int = 0): List[(ActorRef, View, Boolean, Int)] = {
    views.map {
      v =>
        val actor = ViewManagerActor.actorForView(v)

        if (actor.isTerminated) {
          val createdActor = (actorOf(ViewActor.props(v, settings, self, actionsManagerActor, schemaActor), ViewManagerActor.actorNameForView(v)), v, true, depth)
          createdActor :: actorsForViews(v.dependencies.toList, withDependencies, depth + 1)
        } else {
          if (withDependencies) {
            val existingActor = (actor, v, false, depth)
            existingActor :: actorsForViews(v.dependencies.toList, withDependencies, depth + 1)
          } else {
            List((actor, v, false, depth))
          }
        }
    }.flatten
  }

  def initializeViewActors(vs: List[View], withDependencies: Boolean = false): List[ActorRef] = {
    val resolved = actorsForViews(vs, withDependencies)

    val viewsPerTable = resolved
      .filter { case (_, _, newlyCreated, _) => newlyCreated }
      .map { case (_, view, _, _) => view }
      .distinct
      .groupBy { _.tableName }
      .values
      .map(views => AddPartitions(views.toList))
      .toList

    if (viewsPerTable.size > 0) queryActors(schemaActor, viewsPerTable, settings.schemaTimeout)

    if (withDependencies)
      resolved.map { case (viewActor, _, _, _) => viewActor }.distinct
    else
      resolved.filter { case (_, _, _, depth) => depth == 0 }.map { case (viewActor, _, _, _) => viewActor }.distinct
  }
}

object ViewManagerActor {
  def props(settings: SettingsImpl, actionsManagerActor: ActorRef, schemaActor: ActorRef): Props = Props(classOf[ViewManagerActor], settings: SettingsImpl, actionsManagerActor, schemaActor).withDispatcher("akka.aktor.views-dispatcher")

  def actorNameForView(v: View) = v.urlPath.replaceAll("/", ":")

  def viewForActor(a: ActorRef) =
    View.viewsFromUrl(settings.env, a.path.name.replaceAll(":", "/"), settings.viewAugmentor).head

  def actorForView(v: View) = SodaRootActor.settings.system.actorFor(SodaRootActor.viewManagerActor.path.child(actorNameForView(v)))

}