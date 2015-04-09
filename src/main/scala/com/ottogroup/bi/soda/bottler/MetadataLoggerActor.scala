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

  def receive = LoggingReceive({
    
    case SetViewVersion(view) => {
      crate.setTransformationVersion(view)
      sender ! SchemaActionSuccess()
    }

    case LogTransformationTimestamp(view, timestamp) => {
      crate.setTransformationTimestamp(view, timestamp)
      sender ! SchemaActionSuccess()
    }
  })
}

object MetadataLoggerActor {
  def props(jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) = (Props(classOf[MetadataLoggerActor], jdbcUrl, metaStoreUri, serverKerberosPrincipal)).withDispatcher("akka.actor.metadata-logger-dispatcher")
}