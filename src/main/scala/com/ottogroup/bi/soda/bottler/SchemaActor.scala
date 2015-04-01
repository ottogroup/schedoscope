package com.ottogroup.bi.soda.bottler

import scala.collection.mutable.HashMap

import com.ottogroup.bi.soda.Settings
import com.ottogroup.bi.soda.crate.SchemaManager

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.event.Logging
import akka.event.LoggingReceive

class SchemaActor(partitionWriterActor: ActorRef, jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) extends Actor {
  import context._
  val log = Logging(system, this)

  val crate = SchemaManager(jdbcUrl, metaStoreUri, serverKerberosPrincipal)

  val transformationVersions = HashMap[String, HashMap[String, String]]()
  val transformationTimestamps = HashMap[String, HashMap[String, Long]]()

  def receive = LoggingReceive({
    case AddPartition(view) => try {
      crate.createPartition(view)
      log.debug("Created partition " + view.urlPath)
      sender ! SchemaActionSuccess()
    } catch {
      case e: Throwable => { this.sender ! SchemaActionFailure() }
    }

    case AddPartitions(views) => try {
      log.debug(s"Creating ${views.size} partitions for table ${views.head.tableName}")
      crate.createPartitions(views)
      log.debug(s"Created ${views.size} partitions for table ${views.head.tableName}")
      sender ! SchemaActionSuccess()
    } catch {
      case e: Throwable => {
        log.error("Partition creation failed: " + e.getMessage)
        this.sender ! SchemaActionFailure()
      }
    }

    case CheckViewVersion(view) => try {
      if (!Settings().transformationVersioning) {
        sender ! ViewVersionOk(view)
      } else {
        val versions = transformationVersions.get(view.tableName).getOrElse {
          val version = crate.getTransformationVersions(view)
          transformationVersions.put(view.tableName, version)
          version
        }
        val pKey = crate.getPartitionKey(view)
        if (versions.get(pKey).get.equals(view.transformation().versionDigest()))
          sender ! ViewVersionOk(view)
        else
          sender ! ViewVersionMismatch(view, versions.get(pKey).get)
      }
    } catch {
      case e: Throwable => { e.printStackTrace(); this.sender ! SchemaActionFailure() }
    }

    case SetViewVersion(view) => try {
      val viewTransformationVersions = transformationVersions.get(view.tableName).getOrElse {
        val noVersionsYet = HashMap[String, String]()
        transformationVersions.put(view.tableName, noVersionsYet)
        noVersionsYet
      }

      val pKey = crate.getPartitionKey(view)
      if (viewTransformationVersions.contains(pKey) && viewTransformationVersions.get(pKey).get.equals(view.transformation().versionDigest())) {
        sender ! SchemaActionSuccess()
      } else {
        viewTransformationVersions.put(pKey, view.transformation().versionDigest())

        partitionWriterActor ! SetViewVersion(view)
        sender ! SchemaActionSuccess()
      }
    } catch {
      case e: Throwable => { this.sender ! SchemaActionFailure() }
    }

    case LogTransformationTimestamp(view, timestamp) => try {
      val viewTransformationTimestamps = transformationTimestamps.get(view.tableName).getOrElse {
        val noTimestampsYet = HashMap[String, Long]()
        transformationTimestamps.put(view.tableName, noTimestampsYet)
        noTimestampsYet
      }

      val pKey = crate.getPartitionKey(view)
      viewTransformationTimestamps.put(pKey, timestamp)

      partitionWriterActor ! LogTransformationTimestamp(view, timestamp)
      sender ! SchemaActionSuccess()
    } catch {
      case e: Throwable => { this.sender ! SchemaActionFailure() }
    }

    case GetTransformationTimestamp(view) => try {
      val viewTransformationTimestamps = transformationTimestamps.get(view.tableName).getOrElse {
        val timestampsFromMetastore = crate.getTransformationTimestamps(view)
        transformationTimestamps.put(view.tableName, timestampsFromMetastore)
        timestampsFromMetastore
      }
      val pKey = crate.getPartitionKey(view)
      val partitionTimestamp = viewTransformationTimestamps.get(pKey).get

      sender ! TransformationTimestamp(view, partitionTimestamp)
    } catch {
      case e: Throwable => { this.sender ! SchemaActionFailure() }
    }
  })
}

object SchemaActor {
  def props(schemaWriterDelegateActor: ActorRef, jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) = Props(classOf[SchemaActor], schemaWriterDelegateActor, jdbcUrl, metaStoreUri, serverKerberosPrincipal)
}