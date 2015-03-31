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
import akka.actor.ActorRef

class SchemaActor(partitionWriterActor: ActorRef, jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) extends Actor {
  import context._
  val log = Logging(system, this)
  val crate = SchemaManager(jdbcUrl, metaStoreUri, serverKerberosPrincipal)
  val tableVersions = HashMap[String, String]()
  val transformationTimestamps = HashMap[String,HashMap[String,Long]]()

  def receive = LoggingReceive({
    case AddPartition(view) => try {
      crate.createPartition(view)
      log.debug("Created partition " + view.urlPath)
      sender ! SchemaActionSuccess()
    } catch {
      case e: Throwable => { this.sender ! SchemaActionFailure() }
    }

    case AddPartitions(views) => try {
      log.debug("Creating partitions for table " + views.size)
      crate.createPartitions(views)
      log.debug("Created partitions " + views.size)
      sender ! SchemaActionSuccess()
    } catch {
      case e: Throwable => {
        log.error("Partition creation failed: " + e.getMessage)
        this.sender ! SchemaActionFailure()
      }
    }

    case CheckViewVersion(view) =>
      try {
        if (!Settings().transformationVersioning) {
          sender ! ViewVersionOk(view)
        } else {
          val transformationVersion = if (tableVersions.contains(view.tableName)) {
            tableVersions.get(view.tableName).get
          } else {
            val version = crate.getTransformationVersion(view)
            tableVersions.put(view.tableName, version)
            version
          }
          
          if (transformationVersion.equals(view.transformation().versionDigest()))
            sender ! ViewVersionOk(view)
          else
            sender ! ViewVersionMismatch(view, transformationVersion)
        }
      } catch {
        case e: Throwable => { e.printStackTrace(); this.sender ! SchemaActionFailure() }
      }

    case SetViewVersion(view) => try {
      if (tableVersions.contains(view.tableName) && tableVersions.get(view.tableName).get.equals(view.transformation().versionDigest())) {
        sender ! SchemaActionSuccess()
      } 
      else {
        tableVersions.put(view.tableName, view.transformation().versionDigest())
        partitionWriterActor ! SetViewVersion(view)
        sender ! SchemaActionSuccess()
      }
    } catch {
      case e: Throwable => { this.sender ! SchemaActionFailure() }
    }

    case LogTransformationTimestamp(view, timestamp) => try {
      // clear cache
      if (!transformationTimestamps.get(view.tableName).isDefined)
        transformationTimestamps.put(view.tableName, new HashMap[String,Long]())  
        
      transformationTimestamps.get(view.tableName).get.put(view.partitionValues.mkString("/"), timestamp)
      
      partitionWriterActor ! LogTransformationTimestamp(view, timestamp)
      sender ! SchemaActionSuccess()
    } catch {
      case e: Throwable => { this.sender ! SchemaActionFailure() }
    }

    case GetTransformationTimestamp(view) => try {
      // cache timestamps of all partitions from this table
      if (!transformationTimestamps.get(view.tableName).isDefined)
        transformationTimestamps.put(view.tableName, crate.getTransformationTimestamps(view.dbName, view.n) )      
        
      val timeStamp = transformationTimestamps.get(view.tableName).get.get(view.partitionValues.mkString("/")).get
      
      sender ! TransformationTimestamp(view, timeStamp)
    } catch {
      case e: Throwable => { this.sender ! SchemaActionFailure() }
    }
  })
}

object SchemaActor {
  def props(schemaWriterDelegateActor: ActorRef, jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) = Props(classOf[SchemaActor], schemaWriterDelegateActor, jdbcUrl, metaStoreUri, serverKerberosPrincipal)
}