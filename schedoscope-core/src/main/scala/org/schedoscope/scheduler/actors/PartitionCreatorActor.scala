/**
  * Copyright 2015 Otto (GmbH & Co KG)
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package org.schedoscope.scheduler.actors

import akka.actor.{Actor, Props, actorRef2Scala}
import akka.event.{Logging, LoggingReceive}
import org.schedoscope.scheduler.messages._
import org.schedoscope.schema.SchemaManager

/**
  * Parition creator actors are responsible for creating tables and partitions in the metastore.
  */
class PartitionCreatorActor(jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) extends Actor {

  import context._

  val log = Logging(system, this)

  val schemaManager = SchemaManager(jdbcUrl, metaStoreUri, serverKerberosPrincipal)
  var runningCommand: Option[Any] = None

  /**
    * Before the actor gets restarted, reenqueue the running write command with the schema root actor
    * so it does not get lost.
    */
  override def preRestart(reason: Throwable, message: Option[Any]) {
    if (runningCommand.isDefined)
      self forward runningCommand.get
  }

  /**
    * Message handler
    */
  def receive: Receive = LoggingReceive {


    case c: CheckOrCreateTables => {

      runningCommand = Some(c)

      c.views
        .groupBy { v => (v.dbName, v.n) }
        .map { case (_, views) => views.head }
        .foreach {
          tablePrototype => {
            log.info(s"Checking or creating table for view ${tablePrototype.module}.${tablePrototype.n}")

            if (!schemaManager.schemaExists(tablePrototype)) {
              log.info(s"Table for view ${tablePrototype.module}.${tablePrototype.n} does not yet exist, creating")

              schemaManager.dropAndCreateTableSchema(tablePrototype)
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

      val metadata = schemaManager.getTransformationMetadata(views)

      log.info(s"Created / loaded ${views.size} partitions for table ${views.head.tableName}")

      sender ! TransformationMetadata(metadata)

      runningCommand = None

    }

    case g: GetMetaDataForMaterialize => {

      runningCommand = Some(g)

      val metadata = schemaManager.getTransformationMetadata(List(g.view)).head
      sender ! CommandForView(None, g.view, MetaDataForMaterialize(metadata, g.mode, g.materializeSource))

      runningCommand = None
    }
  }
}


/**
  * Factory for partition creator actors
  */
object PartitionCreatorActor {
  def props(jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) = Props(classOf[PartitionCreatorActor], jdbcUrl, metaStoreUri, serverKerberosPrincipal).withDispatcher("akka.actor.partition-creator-dispatcher")
}