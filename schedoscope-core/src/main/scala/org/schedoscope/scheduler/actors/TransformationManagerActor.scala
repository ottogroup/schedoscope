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
import akka.event.{Logging, LoggingReceive}
import akka.routing._
import org.schedoscope.conf.SchedoscopeSettings
import org.schedoscope.dsl.View
import org.schedoscope.dsl.transformations.{FilesystemTransformation, Transformation}
import org.schedoscope.scheduler.driver.{Driver, RetryableDriverException}
import org.schedoscope.scheduler.messages._
import org.schedoscope.scheduler.utils.BackOffSupervision

import scala.collection.JavaConversions.asScalaSet
import scala.collection.mutable.HashMap
import scala.concurrent.duration._


/**
  * The transformation manager actor serves as a factory for all transformation requests, which are sent by view actors.
  * It pushes all requests to the correspondent transformation type driver router, which, in turn, load balances work
  * among its children, the Driver Actors.
  *
  */
class TransformationManagerActor(settings: SchedoscopeSettings,
                                 bootstrapDriverActors: Boolean) extends Actor {

  import context._

  val log = Logging(system, TransformationManagerActor.this)

  /**
    * Supervision strategy. If a driver actor raises a DriverException, the driver actor will be restarted.
    * If any other exception is raised, it is escalated.
    */
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = -1) {
      case _: RetryableDriverException => Restart
      case _: ActorInitializationException => Stop
      case _ => Escalate
    }


  val driverStates = HashMap[String, TransformationStatusResponse[_]]()
  val driverActorsBackOffSupervision = new BackOffSupervision(
    managerName = "TRANSFORMATION MANAGER ACTOR",
    system = context.system)

  def scheduleTick(managedActor: ActorRef, backOffTime: FiniteDuration) {
    system.scheduler.scheduleOnce(backOffTime, managedActor, "tick")
  }

  def manageDriverLifeCycle(asr: TransformationStatusResponse[_]) {

    if (asr.message == "booted") {
      val transformation = getTransformationName(asr.actor)
      val slot = settings.getDriverSettings(transformation).backOffSlotTime millis
      val delay = settings.getDriverSettings(transformation).backOffMinimumDelay millis

      val backOffTime = driverActorsBackOffSupervision.manageActorLifecycle(
        managedActor = asr.actor,
        backOffSlotTime = slot,
        backOffMinimumDelay = delay)

      scheduleTick(asr.actor, backOffTime)
    }
    driverStates.put(asr.actor.path.toStringWithoutAddress, asr)

  }

  def getTransformationName(actor: ActorRef): String = {
    val router = actor.path.toString
      .slice(self.path.toString.size, actor.path.toString.size)
      .split("/")(1)
    val transformation = router.split("-")(0)
    transformation
  }

  /**
    * Create one driver router per transformation type, which themselves spawn driver actors as required by configured transformation concurrency.
    */
  override def preStart {

    if (bootstrapDriverActors) {
      for (transformation <- Driver.transformationsWithDrivers) {
        actorOf(
          SmallestMailboxPool(
            nrOfInstances = settings.getDriverSettings(transformation).concurrency,
            supervisorStrategy = DriverActor.driverRouterSupervisorStrategy,
            routerDispatcher = "akka.actor.driver-router-dispatcher"
          ).props(routeeProps = DriverActor.props(settings, transformation, self)),
          s"${transformation}-driver"
        )
      }
    }
  }

  /**
    * Message handler
    */
  def receive = LoggingReceive({

    case asr: TransformationStatusResponse[_] => manageDriverLifeCycle(asr)

    case GetTransformations() => sender ! TransformationStatusListResponse(driverStates.values.toList)

    case commandToExecute: DriverCommand =>
      commandToExecute.command match {
        case TransformView(transformation, view) =>
          context.actorSelection(s"${self.path}/${transformation}-driver") forward commandToExecute
        case DeployCommand() =>
          context.actorSelection(s"${self.path}/*-driver/*") forward commandToExecute
        case transformation: Transformation =>
          context.actorSelection(s"${self.path}/${transformation.name}-driver") forward commandToExecute
      }

    case viewToTransform: View =>
      val transformation = viewToTransform.transformation()
      val commandRequest = DriverCommand(TransformView(transformation, viewToTransform), sender)
      context.actorSelection(s"${self.path}/${transformation.name}-driver") forward commandRequest

    case filesystemTransformation: FilesystemTransformation =>
      val driverCommand = DriverCommand(filesystemTransformation, sender)
      context.actorSelection(s"${self.path}/${filesystemTransformation.name}-driver") forward driverCommand

    case deploy: DeployCommand =>
      context.actorSelection(s"${self.path}/*-driver/*") forward DriverCommand(deploy, sender)
  })
}

/**
  * Factory for the actions manager actor.
  */
object TransformationManagerActor {

  def props(settings: SchedoscopeSettings,
            bootstrapDriverActors: Boolean = true) =
    Props(classOf[TransformationManagerActor],
      settings,
      bootstrapDriverActors).withDispatcher("akka.actor.transformation-manager-dispatcher")
}
