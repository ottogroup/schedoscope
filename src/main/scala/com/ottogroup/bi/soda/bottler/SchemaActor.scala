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

  val tableVersions = HashMap[String, String]()
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

    case CheckViewVersion(view) => try {
      if (!Settings().transformationVersioning) {
        sender ! ViewVersionOk(view)
      } else {
        val transformationVersion = tableVersions.get(view.tableName).getOrElse {
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
      } else {
        tableVersions.put(view.tableName, view.transformation().versionDigest())

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

      viewTransformationTimestamps.put(view.partitionValues.mkString("/"), timestamp)

      partitionWriterActor ! LogTransformationTimestamp(view, timestamp)
      sender ! SchemaActionSuccess()
    } catch {
      case e: Throwable => { this.sender ! SchemaActionFailure() }
    }

    case GetTransformationTimestamp(view) => try {
      val viewTransformationTimestamps = transformationTimestamps.get(view.tableName).getOrElse {
        val timestampsFromMetastore = crate.getTransformationTimestamps(view.dbName, view.n)
        transformationTimestamps.put(view.tableName, timestampsFromMetastore)
        timestampsFromMetastore
      }

      val partitionTimestamp = viewTransformationTimestamps.get(view.partitionValues.mkString("/")).get

      sender ! TransformationTimestamp(view, partitionTimestamp)
    } catch {
      case e: Throwable => { this.sender ! SchemaActionFailure() }
    }
  })
}

object SchemaActor {
  def props(schemaWriterDelegateActor: ActorRef, jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) = Props(classOf[SchemaActor], schemaWriterDelegateActor, jdbcUrl, metaStoreUri, serverKerberosPrincipal)
}