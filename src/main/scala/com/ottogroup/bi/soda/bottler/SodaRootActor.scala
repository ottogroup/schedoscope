package com.ottogroup.bi.soda.bottler

import scala.concurrent.duration.DurationInt
import com.ottogroup.bi.soda.Settings
import com.ottogroup.bi.soda.SettingsImpl
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.AllForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy.Restart
import scala.concurrent.Await
import akka.actor.ActorSelection
import java.util.Date
import akka.event.Logging

class SodaRootActor(settings: SettingsImpl) extends Actor {
      
  import context._
  
  val log = Logging(system, SodaRootActor.this)
  
  var actionsManagerActor: ActorRef = null
  var schemaActor: ActorRef = null
  var viewManagerActor: ActorRef = null
  var partitionWriterActor: ActorRef = null

  override val supervisorStrategy =
    AllForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _: Throwable => Restart
    }

  override def preStart {
    actionsManagerActor = actorOf(ActionsManagerActor.props(settings.hadoopConf).withDispatcher("akka.actor.actions-manager-dispatcher"), "actions")
    partitionWriterActor = actorOf(PartitionDataWriterActor.props(settings.jdbcUrl, settings.metastoreUri, settings.kerberosPrincipal).withDispatcher("akka.actor.default-dispatcher"), "schema-writer-delegate")
    schemaActor = actorOf(SchemaActor.props(partitionWriterActor, settings.jdbcUrl, settings.metastoreUri, settings.kerberosPrincipal).withDispatcher("akka.actor.default-dispatcher"), "schema")
    viewManagerActor = actorOf(ViewManagerActor.props(settings, actionsManagerActor, schemaActor).withDispatcher("akka.actor.view-manager-dispatcher"), "views")
  }

  def receive = {
    // we do not process any messages as we are merely a supervisor
    case _ => {}
  }

}

object SodaRootActor {
  def props(settings: SettingsImpl) = Props(classOf[SodaRootActor], settings)

  lazy val settings = Settings()

  lazy val sodaRootActor = {
    val ref = settings.system.actorOf(props(settings), "root")
    val fut = settings.system.actorSelection(ref.path).resolveOne(10 seconds)
    Await.result(fut, 10 seconds)
  }

  def actorSelectionToRef(actorSelection: ActorSelection) =
    Await.result(actorSelection.resolveOne(settings.viewManagerResponseTimeout), settings.viewManagerResponseTimeout)

  lazy val viewManagerActor = {
    val system = settings.system
    val root = sodaRootActor    
    actorSelectionToRef(system.actorSelection(root.path.child("views")))    
  }

  lazy val schemaActor ={
    val system = settings.system
    val root = sodaRootActor    
    actorSelectionToRef(system.actorSelection(root.path.child("schema")))    
  }     

  lazy val partitionWriterActor = {
    val system = settings.system
    val root = sodaRootActor    
    actorSelectionToRef(system.actorSelection(root.path.child("schema-writer-delegate")))    
  } 
    

  lazy val actionsManagerActor = {
    val system = settings.system
    val root = sodaRootActor    
    actorSelectionToRef(system.actorSelection(root.path.child("actions")))    
  }
}