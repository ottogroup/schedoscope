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

class SodaRootActor(settings: SettingsImpl) extends Actor {
  import context._

  var actionsManagerActor: ActorRef = null
  var schemaActor: ActorRef = null
  var viewManagerActor: ActorRef = null

  override val supervisorStrategy =
    AllForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _: Throwable => Restart
    }

  override def preStart {
    actionsManagerActor = actorOf(ActionsManagerActor.props(settings.hadoopConf), "actions")
    schemaActor = actorOf(SchemaActor.props(settings.jdbcUrl, settings.metastoreUri, settings.kerberosPrincipal), "schema")
    viewManagerActor = actorOf(ViewManagerActor.props(settings, actionsManagerActor, schemaActor), "views")
  }

  def receive = {
    // we do not process any messages as we are merely a supervisor
    case _ => {}
  }
}

object SodaRootActor {
  def props(settings: SettingsImpl) = Props(classOf[SodaRootActor], settings)

  lazy val settings = Settings()

  lazy val sodaRootActor = settings.system.actorOf(props(settings), "root")

  def actorSelectionToRef(actorSelection: ActorSelection) =
    Await.result(actorSelection.resolveOne(settings.viewManagerResponseTimeout), settings.viewManagerResponseTimeout)

  lazy val viewManagerActor = actorSelectionToRef(settings.system.actorSelection(sodaRootActor.path.child("views")))

  lazy val schemaActor = actorSelectionToRef(settings.system.actorSelection(sodaRootActor.path.child("schema")))

  lazy val actionsManagerActor = actorSelectionToRef(settings.system.actorSelection(sodaRootActor.path.child("actions")))
}