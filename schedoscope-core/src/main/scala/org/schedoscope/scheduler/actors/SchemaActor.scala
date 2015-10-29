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

import org.schedoscope.SchedoscopeSettings
import org.schedoscope.scheduler.messages._
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.OneForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy.Restart
import akka.event.Logging
import akka.routing.RoundRobinPool

/**
 * Supervisor and forwarder for partition creator and metadata logger actors
 */
class SchemaActor(settings: SchedoscopeSettings) extends Actor {
  import context._

  val log = Logging(system, this)

  var metadataLoggerActor: ActorRef = null
  var partitionCreatorActor: ActorRef = null

  /**
   * Supervisor strategy: Restart failing schema or metadata logger actors
   */
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = -1) {
      case _: Throwable => Restart
    }

  override def preStart {
    metadataLoggerActor = actorOf(MetadataLoggerActor.props(settings.jdbcUrl, settings.metastoreUri, settings.kerberosPrincipal), "metadata-logger")
    partitionCreatorActor = actorOf(PartitionCreatorActor.props(settings.jdbcUrl, settings.metastoreUri, settings.kerberosPrincipal).withRouter(new RoundRobinPool(settings.metastoreConcurrency)), "partition-creator")
  }

  def receive = {
    case m: CheckOrCreateTables        => partitionCreatorActor forward m

    case a: AddPartitions              => partitionCreatorActor forward a

    case s: SetViewVersion             => metadataLoggerActor forward s

    case l: LogTransformationTimestamp => metadataLoggerActor forward l
  }
}

object SchemaActor {
  def props(settings: SchedoscopeSettings) = Props(classOf[SchemaActor], settings).withDispatcher("akka.actor.schema-actor-dispatcher")
}
