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
package org.schedoscope.scheduler

import scala.collection.mutable.HashMap
import org.schedoscope.schema.SchemaManager
import akka.actor.Actor
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.event.Logging
import akka.event.LoggingReceive
import akka.routing.SmallestMailboxRoutingLogic
import akka.routing.Router
import akka.routing.RoundRobinRouter
import org.schedoscope.scheduler.messages._

/**
 * The metadata logger actor writes view version checksums and timestamps to the metastore
 */
class MetadataLoggerActor(jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) extends Actor {
  import context._
  val log = Logging(system, MetadataLoggerActor.this)

  val crate = SchemaManager(jdbcUrl, metaStoreUri, serverKerberosPrincipal)
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
   * Message handler.
   */
  def receive = LoggingReceive({

    case s: SetViewVersion => {
      runningCommand = Some(s)
      crate.setTransformationVersion(s.view)
      sender ! SchemaActionSuccess()
      runningCommand = None
    }

    case l: LogTransformationTimestamp => {
      runningCommand = Some(l)
      crate.setTransformationTimestamp(l.view, l.timestamp)
      sender ! SchemaActionSuccess()
      runningCommand = None
    }
  })
}

/**
 * Factory for metadata logger actors.
 */
object MetadataLoggerActor {
  def props(jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) = (Props(classOf[MetadataLoggerActor], jdbcUrl, metaStoreUri, serverKerberosPrincipal)).withDispatcher("akka.actor.metadata-logger-dispatcher")
}
