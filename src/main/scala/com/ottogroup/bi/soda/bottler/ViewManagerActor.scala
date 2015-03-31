package com.ottogroup.bi.soda.bottler

import scala.collection.mutable.ListBuffer
import com.ottogroup.bi.soda.SettingsImpl
import com.ottogroup.bi.soda.bottler.SodaRootActor.settings
import com.ottogroup.bi.soda.dsl.View
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.contrib.pattern.Aggregator
import java.util.UUID
import akka.event.Logging
import akka.event.LoggingReceive
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import scala.collection.mutable.HashSet
import scala.concurrent.duration.DurationInt
import scala.collection.mutable.HashMap


class ViewManagerActor(settings: SettingsImpl, actionsManagerActor: ActorRef, schemaActor: ActorRef) extends Actor {
  import context._
  val log = Logging(system, ViewManagerActor.this)
    
  val viewStatusMap = HashMap[String,ViewStatusResponse]()

  override def preRestart(reason: Throwable, message: Option[Any]) {
    // prevent termination of children during restart and cause their own restart
  }

  def receive = LoggingReceive({
    
    case vsr: ViewStatusResponse => viewStatusMap.put(vsr.view.urlPath, vsr)
    
    case GetStatus() => sender ! ViewStatusListResponse(viewStatusMap.values.toList)
    
    case GetViewStatus(views, withDependencies) => {
      val viewUrls = views.map(v => v.urlPath).toSet
      initializeViewActors(views, withDependencies)
      sender ! ViewStatusListResponse(viewStatusMap.filter( url => viewUrls.contains(url._1)).values.toList)
    }

    case NewDataAvailable(view) => children.filter { _ != sender }.foreach { _ ! NewDataAvailable(view) }

    case ViewList(views) => {
      sender ! initializeViewActors(views, false)
    }

    case v: View => {
      sender ! initializeViewActors(List(v), false).headOption.getOrElse(List())
    }
  })

  def actorsForViews(vs: List[View], forceDependencies: Boolean = false, depth: Int = 0): List[(ActorRef, View, Boolean, Int)] = {
    vs.map {
      v =>
        val actor = ViewManagerActor.actorForView(v)
        if (actor.isTerminated) {
          val createdActor = (actorOf(ViewActor.props(v, settings, self, actionsManagerActor, schemaActor), ViewManagerActor.actorNameForView(v)), v, true, depth)
          actorsForViews(v.dependencies.toList, forceDependencies, depth+1) ++ List(createdActor)
        } else {
          if (forceDependencies) { 
            val existingActor = (actor, v, false, depth)
            actorsForViews(v.dependencies.toList, forceDependencies, depth+1) ++ List(existingActor)
          } else List((actor, v, false, depth))
        }
    }.flatten
  }

  def initializeViewActors(vs: List[View], forceDependencies: Boolean = false): List[ActorRef] = {
    val resolved = actorsForViews(vs, forceDependencies)
    
    val viewsPerTable = resolved
      .filter { _._3 }
      .map { _._2 }
      .distinct
      .groupBy { _.tableName }
      .values
      .map(perTable => AddPartitions(perTable.toList)).toList

    if (viewsPerTable.size > 0) queryActors(schemaActor, viewsPerTable, settings.schemaTimeout)

    if (forceDependencies)
      resolved.map( _._1).distinct
    else
      resolved.filter( _._4 == 0).map( _._1).distinct
  }
}

object ViewManagerActor {
  def props(settings: SettingsImpl, actionsManagerActor: ActorRef, schemaActor: ActorRef): Props = Props(classOf[ViewManagerActor], settings: SettingsImpl, actionsManagerActor, schemaActor).withDispatcher("akka.aktor.views-dispatcher")

  def actorNameForView(v: View) = v.urlPath.replaceAll("/", ":")

  def viewForActor(a: ActorRef) =
    View.viewsFromUrl(settings.env, a.path.name.replaceAll(":", "/"), settings.viewAugmentor).head

  def actorForView(v: View) = SodaRootActor.settings.system.actorFor(SodaRootActor.viewManagerActor.path.child(actorNameForView(v)))

}