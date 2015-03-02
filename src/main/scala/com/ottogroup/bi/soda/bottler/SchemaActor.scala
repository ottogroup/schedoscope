package com.ottogroup.bi.soda.bottler

import com.ottogroup.bi.soda.dsl.View
import akka.actor.Actor
import akka.actor.ActorDSL._
import akka.actor.ActorSystem
import akka.actor.Props
import com.ottogroup.bi.soda.crate.DeploySchema
import com.ottogroup.bi.soda.crate.ddl.HiveQl._

case class CreateSchema(view: View)
case class AddPartition(view: View)

class SchemaActor(jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) extends Actor {
  val crate = DeploySchema(jdbcUrl, metaStoreUri, serverKerberosPrincipal)

  def receive = {
    case CreateSchema(view) => {
      if (!(crate.schemaExists(view.env + "_" + view.module, view.n, ddl(view))))
        crate.dropAndCreateTableSchema(view.env + "_" + view.module, view.n, ddl(view))
      sender ! new Success
    }
    case AddPartition(view) => {
      
      //if (crate.schemaExists(view.env + "_" + view.module, view.n, ddl(view)))
      try {
        crate.createPartition(view)
      } catch {
        case e: Throwable => { this.sender ! Error }
      }
      sender ! new Success
    }
    case CheckVersion(view) => {
      
      if (crate.partitionExists(view.env + "_" + view.module, view.n, ddl(view), view.partitionSpec)) {
        try {

          val digest = crate.getPartitionVersion(view)
          if (digest.equals(view.transformation().versionDigest))
            sender ! VersionOk(view)
          else
            sender ! VersionMismatch(view, digest)
        } catch {
          case e: Throwable => { e.printStackTrace(); this.sender ! Error }
        }
      } else
        sender ! VersionMismatch(view, "")
    }
    case SetVersion(view) => {
      //if (crate.schemaExists(view.env + "_" + view.module, view.n, ddl(view)))
      try {
        crate.setPartitionVersion(view)
      } catch {
        case e: Throwable => { this.sender ! Error }
      }
      sender ! new Success

    }
  }
}

object SchemaActor {
  def props(jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) = Props(new SchemaActor(jdbcUrl, metaStoreUri, serverKerberosPrincipal))

}