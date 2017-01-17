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

import akka.actor.{Actor, ActorInitializationException, ActorRef, OneForOneStrategy, Props}
import akka.actor.SupervisorStrategy._
import akka.event.{Logging, LoggingReceive}
import akka.routing.{ActorRefRoutee, BalancingPool, Router}
import org.schedoscope.conf.SchedoscopeSettings
import org.schedoscope.dsl.View
import org.schedoscope.dsl.transformations.{FilesystemTransformation, Transformation}
import org.schedoscope.scheduler.driver.{Driver, RetryableDriverException}
import org.schedoscope.scheduler.messages._

import scala.collection.JavaConversions.asScalaSet
import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.util.Random

/**
  * The transformation manager actor queues transformation requests it receives from view actors by
  * transformation type. Idle driver actors poll the transformation manager for new transformations to perform.
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
      case _: ActorInitializationException => Restart
      case _ => Escalate
    }

  // used for determining BalancingDispatcher children' Supervision
  lazy val driverManagersupervisorStrategy = OneForOneStrategy(maxNrOfRetries = -1) {
    case _: RetryableDriverException => Restart
    case _: ActorInitializationException => Restart
    case _ => Escalate
  }

  val driverStates = HashMap[String, TransformationStatusResponse[_]]()

  // create a queue for each driver that is not a filesystem driver

  /*
  val nonFilesystemQueues: Map[String, mutable.Queue[DriverCommand]] = Driver.transformationsWithDrivers.filter {
    _ != "filesystem"
  }.foldLeft(Map[String, collection.mutable.Queue[DriverCommand]]()) {
    (nonFilesystemQueuesSoFar, transformationName) =>
      nonFilesystemQueuesSoFar + (transformationName -> new collection.mutable.Queue[DriverCommand]())
  }

  val filesystemConcurrency = settings.getDriverSettings("filesystem").concurrency

  val filesystemQueues = (0 until filesystemConcurrency).foldLeft(Map[String, collection.mutable.Queue[DriverCommand]]()) {
    (filesystemQueuesSoFar, n) => filesystemQueuesSoFar + (s"filesystem-${n}" -> new collection.mutable.Queue[DriverCommand]())
  }

  val queues = nonFilesystemQueues ++ filesystemQueues

  val randomizer = Random

  def hash(s: String) = Math.max(0,
    s.hashCode().abs % filesystemConcurrency)

  def queueNameForTransformation(t: Transformation, s: ActorRef) =
    if (t.name != "filesystem")
      t.name
    else {
      val h = s"filesystem-${hash(s.path.name)}"
      log.debug("computed hash: " + h + " for " + s.path.name)
      h
    }

  def queueNameForTransformationType(transformationType: String) =
    if (transformationType != "filesystem") {
      transformationType
    } else {
      val allFilesystemQueuesEmpty = filesystemQueues.values.forall(currentQueue => currentQueue.isEmpty)

      if (allFilesystemQueuesEmpty)
        "filesystem-0"
      else {
        var foundNonEmptyQueue = false
        var randomPick = ""

        while (!foundNonEmptyQueue) {
          randomPick = s"filesystem-${randomizer.nextInt(filesystemConcurrency)}"
          foundNonEmptyQueue = !queues.get(randomPick).isEmpty
        }

        randomPick
      }
    }

  def transformationQueueStatus() = {
    queues.map(q => (q._1, q._2.map(c => c.command).toList))
  }
  */

  /**
    * Create driver actors as required by configured transformation types and their concurrency.
    */
  override def preStart {
    /*
    if (bootstrapDriverActors) {
      for (transformation <- Driver.transformationsWithDrivers; c <- 0 until settings.getDriverSettings(transformation).concurrency) {
        actorOf(DriverActor.props(settings, transformation, self), s"${transformation}-${c + 1}")
      }
    }
    */

    // TODO: self? | routees namespace ?
    if(bootstrapDriverActors) {

      for(transformation <- Driver.transformationsWithDrivers) {
        actorOf(
          BalancingPool(settings.getDriverSettings(transformation).concurrency,
            supervisorStrategy = driverManagersupervisorStrategy
          ).props(routeeProps = DriverActor.props(settings, transformation, self)),
          s"${transformation}-router")
      }
    }

  }

  /**
    * Message handler
    */
  def receive = LoggingReceive({

    case asr: TransformationStatusResponse[_] => driverStates.put(asr.actor.path.toStringWithoutAddress, asr)

    case GetTransformations() => sender ! TransformationStatusListResponse(driverStates.values.toList)

    /*
    case GetQueues() => sender ! QueueStatusListResponse(transformationQueueStatus())

    case PollCommand(transformationType) => {
      val queueForType = queues(queueNameForTransformationType(transformationType))

      if (queueForType.nonEmpty) {
        val cmd = queueForType.dequeue()

        sender ! cmd

        cmd.command match {
          case TransformView(transformation, _) =>
            log.info(s"TRANSFORMATIONMANAGER DEQUEUE: Dequeued ${transformationType} transformation${if (transformation.view.isDefined) s" for view ${transformation.view.get}" else ""}; queue size is now: ${queueForType.size}")
          case transformation: Transformation =>
            log.info(s"TRANSFORMATIONMANAGER DEQUEUE: Dequeued ${transformationType} transformation${if (transformation.view.isDefined) s" for view ${transformation.view.get}" else ""}; queue size is now: ${queueForType.size}")
          case DeployCommand() =>
            log.info("TRANSFORMATIONMANAGER DEQUEUE: Dequeued deploy action")
        }
      }
    }
    */

    case commandToExecute: DriverCommand =>
      commandToExecute.command match {
        case TransformView(transformation, view) =>
          context.actorSelection(s"${self.path}/${transformation}-router") forward commandToExecute
          //enqueueTransformation(commandToExecute, transformation)
        case DeployCommand() =>
          context.actorSelection(s"${self.path}/*-router") forward commandToExecute
          //enqueueDeploy(commandToExecute)
        case transformation: Transformation =>
          context.actorSelection(s"${self.path}/${transformation.name}-router") forward commandToExecute
          //enqueueTransformation(commandToExecute, transformation)
      }


    case viewToTransform: View =>
      val transformation = viewToTransform.transformation().forView(viewToTransform)
      val commandRequest = DriverCommand(TransformView(transformation, viewToTransform), sender)
      // TODO: forward!
      context.actorSelection(s"${self.path}/${transformation.name}-router") forward commandRequest
      //enqueueTransformation(commandRequest, transformation)

    case filesystemTransformation: FilesystemTransformation =>
      val driverCommand = DriverCommand(filesystemTransformation, sender)
      // TODO: forward!
      context.actorSelection(s"${self.path}/${filesystemTransformation.name}-router") forward driverCommand
      //enqueueTransformation(driverCommand, filesystemTransformation)

    case deploy: DeployCommand =>
      //// TODO: forward!
      println(s"yo: sender ${sender().path}")
      context.actorSelection(s"${self.path}/*-router") forward DriverCommand(deploy, sender)
      //enqueueDeploy(DriverCommand(deploy, sender))
  })

  /*
  def enqueueTransformation(commandToExecute: DriverCommand, transformation: Transformation): Unit = {
    val queueName = queueNameForTransformation(transformation, commandToExecute.sender)

    queues(queueName).enqueue(commandToExecute)
    log.info(s"TRANSFORMATIONMANAGER ENQUEUE: Enqueued ${queueName} transformation${if (transformation.view.isDefined) s" for view ${transformation.view.get}" else ""}; queue size is now: ${queues.get(queueName).get.size}")
  }

  def enqueueDeploy(driverCommand: DriverCommand ): Unit = {
    queues.values.foreach {
      _.enqueue(driverCommand)
    }
    log.info("TRANSFORMATIONMANAGER ENQUEUE: Enqueued deploy action")
  }
  */

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
