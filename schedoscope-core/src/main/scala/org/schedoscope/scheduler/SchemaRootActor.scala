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

import org.schedoscope.SettingsImpl
import org.schedoscope.scheduler.messages._
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.OneForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy.Restart
import akka.event.Logging
import akka.routing.RoundRobinRouter
import org.schedoscope.Settings
import akka.actor.ActorSelection
import scala.concurrent.Await

class SchemaRootActor(settings: SettingsImpl) extends Actor {
  import context._

  val log = Logging(system, this)

  var metadataLoggerActor: ActorRef = null
  var schemaActor: ActorRef = null

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = -1) {
      case _: Throwable => Restart
    }

  override def preStart {
    metadataLoggerActor = actorOf(MetadataLoggerActor.props(settings.jdbcUrl, settings.metastoreUri, settings.kerberosPrincipal), "metadata-logger")
    schemaActor = actorOf(SchemaActor.props(settings.jdbcUrl, settings.metastoreUri, settings.kerberosPrincipal).withRouter(new RoundRobinRouter(settings.metastoreConcurrency)), "schema")
  }

  def receive = {
    case m: CheckOrCreateTables        => schemaActor forward m

    case a: AddPartitions              => schemaActor forward a

    case s: SetViewVersion             => metadataLoggerActor forward s

    case l: LogTransformationTimestamp => metadataLoggerActor forward l
  }
}

object SchemaRootActor {
  def props(settings: SettingsImpl) = Props(classOf[SchemaRootActor], settings).withDispatcher("akka.actor.schema-root-actor-dispatcher")
}
