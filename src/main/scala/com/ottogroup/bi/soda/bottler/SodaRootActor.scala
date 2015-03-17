package com.ottogroup.bi.soda.bottler

import akka.actor.Actor
import akka.actor.ActorRef
import com.ottogroup.bi.soda.bottler.api.SettingsImpl
import akka.actor.Props
import com.ottogroup.bi.soda.bottler.api.Settings
import akka.actor.AllForOneStrategy
import scala.concurrent.duration.DurationInt
import akka.actor.SupervisorStrategy.Restart

class SodaRootActor(settings: SettingsImpl) extends Actor {
  import context._

  var actionsManagerActor: ActorRef = null
  var schemaActor: ActorRef = null
  var viewManagerActor: ActorRef = null

  override val supervisorStrategy =
    AllForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _: Throwable => Restart
    }
  
  override def preStart() {
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
  lazy val sodaRootActor = settings.system.actorOf(props(settings), "soda")
  lazy val viewManagerActor = settings.system.actorFor(sodaRootActor.path.child("views"))
  lazy val schemaActor = settings.system.actorFor(sodaRootActor.path.child("schema"))
  lazy val actionsManagerActor = settings.system.actorFor(sodaRootActor.path.child("actions"))
}