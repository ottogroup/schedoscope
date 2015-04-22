package com.ottogroup.bi.soda.bottler

import scala.collection.mutable.HashMap
import com.ottogroup.bi.soda.crate.SchemaManager
import akka.actor.Actor
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.event.Logging
import akka.event.LoggingReceive
import akka.routing.SmallestMailboxRoutingLogic
import akka.routing.Router
import akka.routing.RoundRobinRouter

class MetadataLoggerActor(jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) extends Actor {
  import context._
  val log = Logging(system, MetadataLoggerActor.this)

  val crate = SchemaManager(jdbcUrl, metaStoreUri, serverKerberosPrincipal)
  var runningCommand: Option[Any] = None

  override def preRestart(reason: Throwable, message: Option[Any]) {
    if (runningCommand.isDefined)
      self forward runningCommand.get
  }

  def receive = LoggingReceive({

    case s: SetViewVersion => {
      runningCommand = Some(s)
      crate.setTransformationVersion(s.view)
      sender ! SchemaActionSuccess()
      runningCommand = None
    }

    case l: LogTransformationTimestamp => {
      runningCommand = Some(l)
      crate.setTransformationTimestamp(l.view, l.timestamp)
      sender ! SchemaActionSuccess()
      runningCommand = None
    }
  })
}

object MetadataLoggerActor {
  def props(jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) = (Props(classOf[MetadataLoggerActor], jdbcUrl, metaStoreUri, serverKerberosPrincipal)).withDispatcher("akka.actor.metadata-logger-dispatcher")
}