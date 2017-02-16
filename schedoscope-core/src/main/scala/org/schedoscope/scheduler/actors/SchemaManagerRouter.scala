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

import akka.actor.SupervisorStrategy._
import akka.actor.{Actor, ActorInitializationException, ActorRef, OneForOneStrategy, Props}
import akka.event.Logging
import akka.routing.RoundRobinPool
import org.schedoscope.conf.SchedoscopeSettings
import org.schedoscope.scheduler.messages._
import org.schedoscope.scheduler.utils.{BackOffSupervision, ExponentialBackOff}
import org.schedoscope.schema.RetryableSchemaManagerException

import scala.concurrent.duration._

/**
  * Supervisor and forwarder for partition creator and metadata logger actors
  */
class SchemaManagerRouter(settings: SchedoscopeSettings) extends Actor {

  import context._

  val log = Logging(system, this)

  val metastoreActorsBackOffSupervision = new BackOffSupervision(
      managerName = "SCHEMA MANAGER ACTOR",
      system = context.system)

  var metadataLoggerActor: ActorRef = null
  var partitionCreatorActor: ActorRef = null

  /**
    * Supervisor strategy: Restart schema or metadata logger actors failing with SchemaManagerExceptions
    */
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = -1) {
      case _: RetryableSchemaManagerException => Restart
      case _: ActorInitializationException => Restart
      case _ => Escalate
    }

  override def preStart {
    metadataLoggerActor = actorOf(
      MetadataLoggerActor.props(settings.jdbcUrl, settings.metastoreUri, settings.kerberosPrincipal),
      "metadata-logger")
    partitionCreatorActor = actorOf(
      PartitionCreatorActor.props(settings.jdbcUrl, settings.metastoreUri, settings.kerberosPrincipal, self)
        .withRouter(new RoundRobinPool(settings.metastoreConcurrency)),
      "partition-creator")
  }

  def scheduleTick(managedActor: ActorRef, backOffTime: FiniteDuration) {
    system.scheduler.scheduleOnce(backOffTime, managedActor, "tick")
  }

  def manageActorLifecycle(metaActor: ActorRef) {
    val slot = settings.backOffSlotTime millis
    val delay = settings.backOffMinimumDelay millis

    val backOffTime = metastoreActorsBackOffSupervision.manageActorLifecycle(
      managedActor = metaActor,
      backOffSlotTime = slot,
      backOffMinimumDelay = delay)

    scheduleTick(metaActor, backOffTime)
  }


  def receive = {

    case "tick" => manageActorLifecycle(sender)

    case m: CheckOrCreateTables => partitionCreatorActor forward m

    case a: AddPartitions => partitionCreatorActor forward a

    case s: SetViewVersion => metadataLoggerActor forward s

    case l: LogTransformationTimestamp => metadataLoggerActor forward l

    case g: GetMetaDataForMaterialize => partitionCreatorActor forward g

  }
}

object SchemaManagerRouter {
  def props(settings: SchedoscopeSettings) = (Props(classOf[SchemaManagerRouter], settings)).withDispatcher("akka.actor.schema-manager-dispatcher")
}
