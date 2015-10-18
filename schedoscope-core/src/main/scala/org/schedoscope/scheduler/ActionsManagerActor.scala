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

import scala.annotation.migration
import scala.collection.JavaConversions.asScalaSet
import scala.collection.mutable.HashMap
import scala.concurrent.duration.DurationInt
import scala.util.Random

import org.apache.hadoop.conf.Configuration

import org.schedoscope.Settings
import org.schedoscope.scheduler.driver.DriverException
import org.schedoscope.dsl.Transformation
import org.schedoscope.dsl.View
import org.schedoscope.dsl.transformations.FilesystemTransformation
import org.schedoscope.scheduler.messages._
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
 *
 */
class ActionsManagerActor() extends Actor {
  import context._

  val log = Logging(system, ActionsManagerActor.this)
  val settings = Settings.get(system)

  val driverStates = HashMap[String, ActionStatusResponse[_]]()

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

  /**
   * @param s
   * @return
   */
  def hash(s: String) = Math.max(0,
    s.hashCode().abs % filesystemConcurrency)

  /**
   * @param t
   * @param s
   * @return
   */
  def queueNameForTransformationAction(t: Transformation, s: ActorRef) =
    if (t.name != "filesystem")
      t.name
    else {
      val h = s"filesystem-${hash(s.path.name)}"
      log.debug("computed hash: " + h + " for " + s.path.name)
      h
    }

  /**
   * @param transformationType
   * @return
   */
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

  private def actionQueueStatus() = {
    queues.map(q => (q._1, q._2.map(c => c.command).toList))
  }
  /**
   * How to handle Exceptions. Drivers will be restarted, all other exceptions
   * escalated to supervisor
   */
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = -1) {
      case _: DriverException => Restart
      case _: Throwable       => Escalate
    }

  override def preStart {
    for (transformation <- availableTransformations; c <- 0 until settings.getDriverSettings(transformation).concurrency) {
      actorOf(DriverActor.props(transformation, self), s"${transformation}-${c + 1}")
    }
  }

  def receive = LoggingReceive({

    case asr: ActionStatusResponse[_] => driverStates.put(asr.actor.path.toStringWithoutAddress, asr)

    case GetActions()                 => sender ! ActionStatusListResponse(driverStates.values.toList)

    case GetQueues()                  => sender ! QueueStatusListResponse(actionQueueStatus)

    case PollCommand(transformationType) => {
      val queueForType = queues.get(queueNameForTransformationType(transformationType)).get

      if (!queueForType.isEmpty) {
        val cmd = queueForType.dequeue()

        sender ! cmd

        if (cmd.command.isInstanceOf[Transformation]) {
          val transformation = cmd.command.asInstanceOf[Transformation]
          log.info(s"ACTIONMANAGER DEQUEUE: Dequeued ${transformationType} transformation ${transformation}${if (transformation.view.isDefined) s" for view ${transformation.view.get}" else ""}; queue size is now: ${queueForType.size}")
        } else
          log.info("ACTIONMANAGER DEQUEUE: Dequeued deploy action")
      }
    }

    case actionCommand: CommandWithSender => {
      if (actionCommand.command.isInstanceOf[Transformation]) {
        val transformation = actionCommand.command.asInstanceOf[Transformation]
        val queueName = queueNameForTransformationAction(transformation, actionCommand.sender)

        queues.get(queueName).get.enqueue(actionCommand)
        log.info(s"ACTIONMANAGER ENQUEUE: Enqueued ${queueName} transformation ${transformation}${if (transformation.view.isDefined) s" for view ${transformation.view.get}" else ""}; queue size is now: ${queues.get(queueName).get.size}")
      } else {
        queues.values.foreach { _.enqueue(actionCommand) }
        log.info("ACTIONMANAGER ENQUEUE: Enqueued deploy action")
      }
    }

    case viewAction: View                                         => self ! CommandWithSender(viewAction.transformation().forView(viewAction), sender)

    case filesystemTransformationAction: FilesystemTransformation => self ! CommandWithSender(filesystemTransformationAction, sender)

    case deployAction: Deploy                                     => self ! CommandWithSender(deployAction, sender)
  })
}

object ActionsManagerActor {
  def props(conf: Configuration) = Props[ActionsManagerActor].withDispatcher("akka.actor.actions-manager-dispatcher")
}
