package com.ottogroup.bi.soda.bottler

import com.ottogroup.bi.soda.SettingsImpl
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.OneForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy.Restart
import akka.event.Logging
import akka.routing.RoundRobinRouter
import com.ottogroup.bi.soda.Settings
import akka.actor.ActorSelection
import scala.concurrent.Await

class SchemaRootActor(settings: SettingsImpl) extends Actor {
  import context._

  val log = Logging(system, this)

  var metadataLoggerActor: ActorRef = null
  var schemaActor: ActorRef = null

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = -1) {
      case _: Throwable => Restart
    }

  override def preStart {
    metadataLoggerActor = actorOf(MetadataLoggerActor.props(settings.jdbcUrl, settings.metastoreUri, settings.kerberosPrincipal), "metadata-logger")
    schemaActor = actorOf(SchemaActor.props(settings.jdbcUrl, settings.metastoreUri, settings.kerberosPrincipal).withRouter(new RoundRobinRouter(settings.metastoreConcurrency)), "schema")
  }

  def receive = {
    case m: CheckOrCreateTables => schemaActor forward m
    
    case a: AddPartitions => schemaActor forward a
    
    case s: SetViewVersion => metadataLoggerActor forward s
    
    case l: LogTransformationTimestamp => metadataLoggerActor forward l
  }
}

object SchemaRootActor {
  def props(settings: SettingsImpl) = Props(classOf[SchemaRootActor], settings).withDispatcher("akka.actor.schema-root-actor-dispatcher")
}