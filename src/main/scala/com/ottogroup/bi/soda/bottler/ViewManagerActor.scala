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
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

class ViewStatusRetriever() extends Actor with Aggregator {
  expectOnce {
    case GetViewStatusList(statusRequester, viewActors) => if (viewActors.isEmpty) {
      statusRequester ! ViewStatusListResponse(List())
      context.stop(self)
    } else
      new MultipleResponseHandler(statusRequester, viewActors)
  }

  class MultipleResponseHandler(statusRequester: ActorRef, viewActors: Seq[ActorRef]) {
    val log = Logging(settings.system, ViewStatusRetriever.this)
    import context.dispatcher
    import collection.mutable.ArrayBuffer

    val receivedViewStats = HashMap[String,ViewStatusResponse]() //  ListBuffer[ViewStatusResponse]()
    
    log.debug("VIEW AGGREGATION: sending getStatus")
    viewActors.foreach(_ ! GetStatus())
    log.debug("VIEW AGGREGATION: sending getStatus done")
    context.system.scheduler.scheduleOnce(settings.statusListAggregationTimeout / 2, self, "timeout")

    val handle = expect {

      case viewStatus: ViewStatusResponse => {        
        receivedViewStats.put(viewStatus.view.urlPath, viewStatus)
        log.debug("VIEW AGGREGATION: received response " + receivedViewStats.size)

        if (receivedViewStats.size == viewActors.size)
          processFinal()
      }

      case "timeout" => {
        log.debug("VIEW AGGREGATION: received timeout")
        processFinal()        
      }
    }

    def processFinal() {
      unexpect(handle)
      viewActors.foreach(va => {
        val view = ViewManagerActor.viewForActor(va)
        if (!receivedViewStats.get(view.urlPath).isDefined)
          receivedViewStats.put(view.urlPath, ViewStatusResponse("no-response", view))
      })
      statusRequester ! ViewStatusListResponse(receivedViewStats.values.toList)
      context.stop(self)
    }
  }
}

class ViewManagerActor(settings: SettingsImpl, actionsManagerActor: ActorRef, schemaActor: ActorRef) extends Actor {
  import context._
  val log = Logging(system, ViewManagerActor.this)

  override def preRestart(reason: Throwable, message: Option[Any]) {
    // prevent termination of children during restart and cause their own restart
  }

  def receive = LoggingReceive({
    case GetStatus() => actorOf(Props[ViewStatusRetriever], "aggregator-" + UUID.randomUUID()) ! GetViewStatusList(sender, children.toList.filter { !_.path.toStringWithoutAddress.contains("aggregator-") })

    case GetViewStatus(views, withDependencies) => {
      val viewActors = initializeViewActors(views, withDependencies)
      actorOf(Props[ViewStatusRetriever], "aggregator-" + UUID.randomUUID()) ! GetViewStatusList(sender, viewActors)
    }

    case NewDataAvailable(view) => children.filter { _ != sender }.foreach { _ ! NewDataAvailable(view) }

    case ViewList(views) => {      
      sender ! initializeViewActors(views, false)
    }
    
    case v: View => {
      sender ! initializeViewActors(List(v), false).head
    }
  })
  
  def initializeViewActors(vl: List[View], withDeps: Boolean) : List[ActorRef] = {
    val initializedViews = HashSet[View]()
    val dependencyActors = HashSet[ActorRef]()
    val actors = vl.map( v => {
      val actor = ViewManagerActor.actorForView(v)
      if (actor.isTerminated) {
        initializeDependencyActors(v, initializedViews, dependencyActors)
        initializedViews.add(v)
        actorOf(ViewActor.props(v, settings, self, actionsManagerActor, schemaActor), ViewManagerActor.actorNameForView(v))        
      }
      else {
        if (withDeps) initializeDependencyActors(v, initializedViews, dependencyActors)
        actor 
      }        
      })
      
    val viewsPerTable = initializedViews
      .groupBy(v => v.tableName)
      .values
      .map(perTable => AddPartitions(perTable.toList)).toList
    queryActors(schemaActor, viewsPerTable, settings.schemaTimeout)
    
    if(withDeps) actors ::: dependencyActors.toList else actors
  } 
  
  def initializeDependencyActors(v: View, initialized: HashSet[View], actors: HashSet[ActorRef]) {
    v.dependencies.foreach( d => {
      var actor = ViewManagerActor.actorForView(d)
      if (actor.isTerminated) {
        actor = actorOf(ViewActor.props(d, settings, self, actionsManagerActor, schemaActor), ViewManagerActor.actorNameForView(d))
        log.debug("Initialized dependency actor " + actor.path.toStringWithoutAddress)
        initialized.add(v)        
      }
      actors.add(actor)
      initializeDependencyActors(d, initialized, actors)
    })    
  }
}

object ViewManagerActor {
  def props(settings: SettingsImpl, actionsManagerActor: ActorRef, schemaActor: ActorRef): Props = Props(classOf[ViewManagerActor], settings: SettingsImpl, actionsManagerActor, schemaActor)

  def actorNameForView(v: View) = v.urlPath.replaceAll("/", ":")
  
  def viewForActor(a: ActorRef) = try {
    View.viewsFromUrl(settings.env, a.path.name.replaceAll(":", "/"), settings.viewAugmentor).head
  } catch {
    case t : Throwable => println("***** Could not instantiate view for actor with path " + a.path.name.replaceAll(":", "/"))
    null
  }

  def actorForView(v: View) = SodaRootActor.settings.system.actorFor(SodaRootActor.viewManagerActor.path.child(actorNameForView(v)))

}