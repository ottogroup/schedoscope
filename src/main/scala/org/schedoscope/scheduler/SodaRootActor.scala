package org.schedoscope.scheduler

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import com.ottogroup.bi.soda.Settings
import com.ottogroup.bi.soda.SettingsImpl
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSelection
import akka.actor.AllForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy._
import akka.event.Logging
import akka.routing.RoundRobinRouter

class SodaRootActor(settings: SettingsImpl) extends Actor {
  import context._

  val log = Logging(system, SodaRootActor.this)

  var actionsManagerActor: ActorRef = null
  var schemaRootActor: ActorRef = null
  var viewManagerActor: ActorRef = null

  override val supervisorStrategy =
    AllForOneStrategy() {
      case _: Throwable => { this.context.system.shutdown(); Escalate }
    }

  override def preStart {
    actionsManagerActor = actorOf(ActionsManagerActor.props(settings.hadoopConf), "actions")
    schemaRootActor = actorOf(SchemaRootActor.props(settings), "schema-root")
    viewManagerActor = actorOf(
      ViewManagerActor.props(settings, actionsManagerActor,
        schemaRootActor,
        schemaRootActor), "views")
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

  lazy val schemaRootActor = actorSelectionToRef(settings.system.actorSelection(sodaRootActor.path.child("schema-root")))

  lazy val schemaActor = actorSelectionToRef(settings.system.actorSelection(schemaRootActor.path.child("schema")))

  lazy val metadataLoggerActor = actorSelectionToRef(settings.system.actorSelection(schemaRootActor.path.child("metadata-logger")))

  lazy val actionsManagerActor = actorSelectionToRef(settings.system.actorSelection(sodaRootActor.path.child("actions")))
}