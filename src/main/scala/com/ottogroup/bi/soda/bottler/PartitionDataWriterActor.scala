package com.ottogroup.bi.soda.bottler

import com.ottogroup.bi.soda.dsl.View
import akka.actor.Actor
import akka.actor.ActorDSL._
import akka.actor.ActorSystem
import akka.actor.Props
import com.ottogroup.bi.soda.crate.SchemaManager
import com.ottogroup.bi.soda.crate.ddl.HiveQl._
import com.ottogroup.bi.soda.Settings
import com.ottogroup.bi.soda.dsl.Version
import java.util.Date
import akka.event.LoggingReceive
import akka.event.Logging
import scala.collection.mutable.HashMap

class PartitionDataWriterActor(jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) extends Actor {
  import context._
  val log = Logging(system, this)
  val crate = SchemaManager(jdbcUrl, metaStoreUri, serverKerberosPrincipal)


  def receive = LoggingReceive({
    case SetViewVersion(view) => crate.setTransformationVersion(view)

    case LogTransformationTimestamp(view, timestamp) => crate.setTransformationTimestamp(view, timestamp)
  })
}

object PartitionDataWriterActor {
  def props(jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) = Props(classOf[PartitionDataWriterActor], jdbcUrl, metaStoreUri, serverKerberosPrincipal)
}