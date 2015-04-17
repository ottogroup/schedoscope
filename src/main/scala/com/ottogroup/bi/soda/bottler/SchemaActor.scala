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

class SchemaActor(jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) extends Actor {
  import context._
  val log = Logging(system, this)

  val crate = SchemaManager(jdbcUrl, metaStoreUri, serverKerberosPrincipal)
  var runningCommand: Option[Any] = None

  override def preRestart(reason: Throwable, message: Option[Any]) {
    if (runningCommand.isDefined)
      self forward runningCommand.get
  }
  
  def receive = LoggingReceive({
    case c: CheckOrCreateTables => {
      runningCommand = Some(c)
      
      c.views
        .groupBy { v => (v.dbName, v.n) }
        .map { case (_, views) => views.head }.filter ( table => table.isExternal)
        .foreach {
          tablePrototype => 
            {
              log.info(s"Checking or creating table for view ${tablePrototype.module}.${tablePrototype.n}")

              if (!crate.schemaExists(tablePrototype)) {
                log.info(s"Table for view ${tablePrototype.module}.${tablePrototype.n} does not yet exist, creating")

                crate.dropAndCreateTableSchema(tablePrototype)
              }
            }
        }

      sender ! SchemaActionSuccess()

      runningCommand = None
    }

    case a: AddPartitions => {
      runningCommand = Some(a)
      
      val views = a.views
      log.info(s"Creating / loading ${views.size} partitions for table ${views.head.tableName}")

      val metadata = crate.getTransformationMetadata(views)

      log.info(s"Created / loaded ${views.size} partitions for table ${views.head.tableName}")

      sender ! TransformationMetadata(metadata)
      
      runningCommand = None
    }
  })
}

object SchemaActor {
  def props(jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) = (Props(classOf[SchemaActor], jdbcUrl, metaStoreUri, serverKerberosPrincipal)).withDispatcher("akka.actor.schema-actor-dispatcher")
}