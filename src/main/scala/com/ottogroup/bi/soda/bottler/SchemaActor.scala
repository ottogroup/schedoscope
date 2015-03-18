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

class SchemaActor(jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) extends Actor {
  val crate = SchemaManager(jdbcUrl, metaStoreUri, serverKerberosPrincipal)

  def receive = {
    case AddPartition(view) => try {
      crate.createPartition(view)
      sender ! SchemaActionSuccess()
    } catch {
      case e: Throwable => { this.sender ! SchemaActionFailure() }
    }

    case CheckViewVersion(view) =>
      try {
        if (crate.partitionExists(view)) {
          if (!Settings().transformationVersioning) {
            sender ! ViewVersionOk(view)
          } else {
            val pv = crate.getPartitionVersion(view)
            if (pv.equals(view.transformation().versionDigest()))
              sender ! ViewVersionOk(view)
            else
              sender ! ViewVersionMismatch(view, pv)
          }
        } else {
          sender ! ViewVersionMismatch(view, "")
        }
      } catch {
        case e: Throwable => { e.printStackTrace(); this.sender ! SchemaActionFailure() }
      }

    case SetViewVersion(view) => try {
      crate.setPartitionVersion(view)
      sender ! SchemaActionSuccess()
    } catch {
      case e: Throwable => { this.sender ! SchemaActionFailure() }
    }

    case LogTransformationTimestamp(view) => try {
      crate.setTransformationTimestamp(view, new Date().getTime())
      sender ! SchemaActionSuccess()
    } catch {
      case e: Throwable => { this.sender ! SchemaActionFailure() }
    }

    case GetTransformationTimestamp(view) => try {
      val timeStamp = crate.getTransformationTimestamp(view)
      sender ! TransformationTimestamp(view, timeStamp)
    } catch {
      case e: Throwable => { this.sender ! SchemaActionFailure() }
    }
  }
}

object SchemaActor {
  def props(jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) = Props(classOf[SchemaActor], jdbcUrl, metaStoreUri, serverKerberosPrincipal)
}