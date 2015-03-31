package com.ottogroup.bi.soda.bottler

import com.ottogroup.bi.soda.crate.SchemaManager

import akka.actor.Actor
import akka.actor.Props
import akka.event.Logging
import akka.event.LoggingReceive

class PartitionMetadataLoggerActor(jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) extends Actor {
  import context._
  val log = Logging(system, PartitionMetadataLoggerActor.this)

  val crate = SchemaManager(jdbcUrl, metaStoreUri, serverKerberosPrincipal)

  def receive = LoggingReceive({
    case SetViewVersion(view) => crate.setTransformationVersion(view)

    case LogTransformationTimestamp(view, timestamp) => crate.setTransformationTimestamp(view, timestamp)
  })
}

object PartitionMetadataLoggerActor {
  def props(jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) = Props(classOf[PartitionMetadataLoggerActor], jdbcUrl, metaStoreUri, serverKerberosPrincipal)
}