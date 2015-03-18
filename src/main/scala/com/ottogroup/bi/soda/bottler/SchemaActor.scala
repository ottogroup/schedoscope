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

class SchemaActor(jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) extends Actor {
  val crate = SchemaManager(jdbcUrl, metaStoreUri, serverKerberosPrincipal)

  def receive = {
    case AddPartition(view) => {
      try {
        crate.createPartition(view)
      } catch {
        case e: Throwable => { this.sender ! SchemaActionFailure() }
      }
      sender ! SchemaActionSuccess()
    }

    case CheckVersion(view) =>
      if (crate.partitionExists(view)) {
        try {
          if (!Settings().transformationVersioning) {
            sender ! VersionOk(view)
          } else {
            val pv = crate.getPartitionVersion(view)
            if (pv.equals(view.transformation().versionDigest()))
              sender ! VersionOk(view)
            else
              sender ! VersionMismatch(view, pv)
          }
        } catch {
          case e: Throwable => { e.printStackTrace(); this.sender ! SchemaActionFailure() }
        }
      } else
        sender ! VersionMismatch(view, "")

    case SetVersion(view) => {
      try {
        crate.setPartitionVersion(view)
      } catch {
        case e: Throwable => { this.sender ! SchemaActionFailure() }
      }
      sender ! SchemaActionSuccess()
    }
  }
}

object SchemaActor {
  def props(jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) = Props(classOf[SchemaActor], jdbcUrl, metaStoreUri, serverKerberosPrincipal)
}