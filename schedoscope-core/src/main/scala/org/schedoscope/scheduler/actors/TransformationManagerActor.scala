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

import scala.collection.mutable.HashMap
import scala.util.Random
import scala.collection.JavaConversions.asScalaSet
import org.apache.hadoop.conf.Configuration
import org.schedoscope.SettingsImpl
import org.schedoscope.dsl.View
import org.schedoscope.dsl.transformations.FilesystemTransformation
import org.schedoscope.dsl.transformations.Transformation
import org.schedoscope.scheduler.driver.DriverException
import org.schedoscope.scheduler.messages.CommandWithSender
import org.schedoscope.scheduler.messages.DeployCommand
import org.schedoscope.scheduler.messages.GetTransformations
import org.schedoscope.scheduler.messages.GetQueues
import org.schedoscope.scheduler.messages.PollCommand
import org.schedoscope.scheduler.messages.QueueStatusListResponse
import org.schedoscope.scheduler.messages.TransformationStatusListResponse
import org.schedoscope.scheduler.messages.TransformationStatusResponse
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.OneForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy.Escalate
import akka.actor.SupervisorStrategy.Restart
import akka.actor.actorRef2Scala
import akka.event.Logging
import akka.event.LoggingReceive

/**
 * The transformation manager actor queues transformation requests it receives from view actors by
 * transformation type. Idle driver actors poll the transformation manager for new transformations to perform.
 *
 */
class TransformationManagerActor(settings: SettingsImpl) extends Actor {
  import context._

  val log = Logging(system, TransformationManagerActor.this)

  val driverStates = HashMap[String, TransformationStatusResponse[_]]()

  val availableTransformations = settings.availableTransformations.keySet()

  // create a queue for each driver that is not a filesystem driver
  val nonFilesystemQueues = availableTransformations.filter { _ != "filesystem" }.foldLeft(Map[String, collection.mutable.Queue[CommandWithSender]]()) {
    (nonFilesystemQueuesSoFar, driverName) =>
      nonFilesystemQueuesSoFar + (driverName -> new collection.mutable.Queue[CommandWithSender]())
  }

  val filesystemConcurrency = settings.getDriverSettings("filesystem").concurrency

  val filesystemQueues = (0 until filesystemConcurrency).foldLeft(Map[String, collection.mutable.Queue[CommandWithSender]]()) {
    (filesystemQueuesSoFar, n) => filesystemQueuesSoFar + (s"filesystem-${n}" -> new collection.mutable.Queue[CommandWithSender]())
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
      val allFilesystemQueuesEmpty = filesystemQueues.values.foldLeft(true) {
        (emptySoFar, currentQueue) => emptySoFar && currentQueue.isEmpty
      }

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

  private def transformationQueueStatus() = {
    queues.map(q => (q._1, q._2.map(c => c.command).toList))
  }

  /**
   * Supervision strategy. If a driver actor raises a DriverException, the driver actor will be restarted.
   * If any other exception is raised, it is escalated.
   */
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = -1) {
      case _: DriverException => Restart
      case _: Throwable       => Escalate
    }

  /**
   * Create driver actors as required by configured transformation types and their concurrency.
   */
  override def preStart {
    for (transformation <- availableTransformations; c <- 0 until settings.getDriverSettings(transformation).concurrency) {
      actorOf(DriverActor.props(settings, transformation, self), s"${transformation}-${c + 1}")
    }
  }

  /**
   * Message handler
   */
  def receive = LoggingReceive({

    case asr: TransformationStatusResponse[_] => driverStates.put(asr.actor.path.toStringWithoutAddress, asr)

    case GetTransformations()                 => sender ! TransformationStatusListResponse(driverStates.values.toList)

    case GetQueues()                          => sender ! QueueStatusListResponse(transformationQueueStatus)

    case PollCommand(transformationType) => {
      val queueForType = queues.get(queueNameForTransformationType(transformationType)).get

      if (!queueForType.isEmpty) {
        val cmd = queueForType.dequeue()

        sender ! cmd

        if (cmd.command.isInstanceOf[Transformation]) {
          val transformation = cmd.command.asInstanceOf[Transformation]
          log.info(s"TRANSFORMATIONMANAGER DEQUEUE: Dequeued ${transformationType} transformation ${transformation}${if (transformation.view.isDefined) s" for view ${transformation.view.get}" else ""}; queue size is now: ${queueForType.size}")
        } else
          log.info("TRANSFORMATIONMANAGER DEQUEUE: Dequeued deploy action")
      }
    }

    case commandToExecute: CommandWithSender => {
      if (commandToExecute.command.isInstanceOf[Transformation]) {
        val transformation = commandToExecute.command.asInstanceOf[Transformation]
        val queueName = queueNameForTransformation(transformation, commandToExecute.sender)

        queues.get(queueName).get.enqueue(commandToExecute)
        log.info(s"TRANSFORMATIONMANAGER ENQUEUE: Enqueued ${queueName} transformation ${transformation}${if (transformation.view.isDefined) s" for view ${transformation.view.get}" else ""}; queue size is now: ${queues.get(queueName).get.size}")
      } else {
        queues.values.foreach { _.enqueue(commandToExecute) }
        log.info("TRANSFORMATIONMANAGER ENQUEUE: Enqueued deploy action")
      }
    }

    case viewToTransform: View                              => self ! CommandWithSender(viewToTransform.transformation().forView(viewToTransform), sender)

    case filesystemTransformation: FilesystemTransformation => self ! CommandWithSender(filesystemTransformation, sender)

    case deploy: DeployCommand                              => self ! CommandWithSender(deploy, sender)
  })
}

/**
 * Factory for the actions manager actor.
 */
object TransformationManagerActor {
  def props(settings: SettingsImpl) = Props(classOf[TransformationManagerActor], settings).withDispatcher("akka.actor.transformation-manager-dispatcher")
}
