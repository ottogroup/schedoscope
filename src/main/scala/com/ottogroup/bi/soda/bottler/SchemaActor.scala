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
    case AddPartitions(views) => try {
      log.debug(s"Creating ${views.size} partitions for table ${views.head.tableName}")
      
//      views.grouped(SodaRootActor.settings.metastoreBatchSize).foreach( batch => {        
//        crate.createPartitions(batch)        
//        log.debug("created partitions batch of size " + batch.size + " for table " + views.head.tableName)
//      })
      
      // this fetches the metadata for existing partitions and creates new ones if not existent
      val metadata = crate.getTransformationMetadata(views)

      log.debug(s"Created ${views.size} partitions for table ${views.head.tableName}")
      log.debug(s" sending metadata: ${metadata}")

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
  def props(jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) = Props(classOf[SchemaActor], jdbcUrl, metaStoreUri, serverKerberosPrincipal).withRouter(new RoundRobinRouter(SodaRootActor.settings.metastoreConcurrency)) // FIXME: Why is RoundRobinRouter deprecated?
}