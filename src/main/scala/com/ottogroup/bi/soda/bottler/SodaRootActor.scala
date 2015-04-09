package com.ottogroup.bi.soda.bottler

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import com.ottogroup.bi.soda.Settings
import com.ottogroup.bi.soda.SettingsImpl
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSelection
import akka.actor.AllForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy.Restart
import akka.event.Logging
import akka.routing.RoundRobinRouter

class SodaRootActor(settings: SettingsImpl) extends Actor {
  import context._

  val log = Logging(system, SodaRootActor.this)

  var actionsManagerActor: ActorRef = null
  var metadataLoggerActor: ActorRef = null
  var schemaActor: ActorRef = null
  var viewManagerActor: ActorRef = null

  
  
  override val supervisorStrategy =
    AllForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _: Throwable => Restart
    }

  override def preStart {
    actionsManagerActor = actorOf(ActionsManagerActor.props(settings.hadoopConf), "actions")
    metadataLoggerActor = actorOf(MetadataLoggerActor.props(settings.jdbcUrl, settings.metastoreUri, settings.kerberosPrincipal), "metadata-logger")
    schemaActor = actorOf(SchemaActor.props(settings.jdbcUrl, settings.metastoreUri, settings.kerberosPrincipal).withRouter(new RoundRobinRouter(settings.metastoreConcurrency)), "schema")
    viewManagerActor = actorOf(ViewManagerActor.props(settings, actionsManagerActor, schemaActor, metadataLoggerActor), "views")
  }

  def receive = {
    // we do not process any messages as we are merely a supervisor
    case _ => {}
  }
}

object SodaRootActor {
  def props(settings: SettingsImpl) = Props(classOf[SodaRootActor], settings).withDispatcher("akka.actor.root-actor-dispatcher")

  lazy val settings = Settings()

  def actorSelectionToRef(actorSelection: ActorSelection) =
    Await.result(actorSelection.resolveOne(settings.viewManagerResponseTimeout), settings.viewManagerResponseTimeout)

  lazy val sodaRootActor = actorSelectionToRef(settings.system.actorSelection(settings.system.actorOf(props(settings), "root").path))

  lazy val viewManagerActor = actorSelectionToRef(settings.system.actorSelection(sodaRootActor.path.child("views")))

  lazy val schemaActor = actorSelectionToRef(settings.system.actorSelection(sodaRootActor.path.child("schema")))

  lazy val metadataLoggerActor = actorSelectionToRef(settings.system.actorSelection(sodaRootActor.path.child("metadata-logger")))
  
  lazy val partitionMetadataLoggerActor = actorSelectionToRef(settings.system.actorSelection(sodaRootActor.path.child("schema-writer-delegate")))

  lazy val actionsManagerActor = actorSelectionToRef(settings.system.actorSelection(sodaRootActor.path.child("actions")))
}