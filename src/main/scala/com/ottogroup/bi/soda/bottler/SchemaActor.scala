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

  val transformationVersions = HashMap[String, HashMap[String, String]]()
  val transformationTimestamps = HashMap[String, HashMap[String, Long]]()

  def receive = LoggingReceive({

    case CheckOrCreateTables(views) => try {
      views
        .groupBy { v => (v.dbName, v.n) }
        .map { case (_, views) => views.head }
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
    } catch {
      case e: Throwable => {
        log.error("Table check failed: " + e.getMessage)
        e.printStackTrace()
        this.sender ! SchemaActionFailure()
      }
    }

    case AddPartitions(views) => try {
      log.info(s"Creating / loading ${views.size} partitions for table ${views.head.tableName}")

      val metadata = crate.getTransformationMetadata(views)

      log.info(s"Created / loaded ${views.size} partitions for table ${views.head.tableName}")

      sender ! TransformationMetadata(metadata)
    } catch {
      case e: Throwable => {
        log.error("Partition creation failed: " + e.getMessage)
        e.printStackTrace()
        this.sender ! SchemaActionFailure()
      }
    }

    case SetViewVersion(view) => try {
      crate.setTransformationVersion(view)
      sender ! SchemaActionSuccess()
    } catch {
      case e: Throwable => { this.sender ! SchemaActionFailure() }
    }

    case LogTransformationTimestamp(view, timestamp) => try {
      crate.setTransformationTimestamp(view, timestamp)
      sender ! SchemaActionSuccess()
    } catch {
      case e: Throwable => { this.sender ! SchemaActionFailure() }
    }
  })
}

object SchemaActor {
  def props(jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) = Props(classOf[SchemaActor], jdbcUrl, metaStoreUri, serverKerberosPrincipal).withDispatcher("akka.actor.views-dispatcher").withRouter(new RoundRobinRouter(SodaRootActor.settings.metastoreConcurrency))
}