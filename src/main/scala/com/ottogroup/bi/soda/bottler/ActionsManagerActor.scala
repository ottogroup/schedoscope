package com.ottogroup.bi.soda.bottler

import scala.Option.option2Iterable
import scala.collection.JavaConversions.asScalaSet
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt

import org.apache.hadoop.conf.Configuration

import com.ottogroup.bi.soda.bottler.api.Settings
import com.ottogroup.bi.soda.bottler.driver.DriverException
import com.ottogroup.bi.soda.bottler.driver.FileSystemDriver
import com.ottogroup.bi.soda.dsl.Transformation
import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.dsl.transformations.filesystem.FilesystemTransformation

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSelection.toScala
import akka.actor.OneForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy.Escalate
import akka.actor.SupervisorStrategy.Restart
import akka.actor.actorRef2Scala
import akka.contrib.pattern.Aggregator
import akka.event.Logging
import akka.event.LoggingReceive

/**
 * This actor aggregrates responses from multiple Actors
 * Used for retrieving running jobs,
 *
 * @author dev_hzorn
 *
 */
class ActionStatusRetriever extends Actor with Aggregator {

  expectOnce {
    case GetProcessList(s, q) => new MultipleResponseHandler(s, q, "")
  }

  class MultipleResponseHandler(originalSender: ActorRef, queues: Map[String, List[String]], propName: String) {
    import context.dispatcher
    import collection.mutable.ArrayBuffer

    val values = ArrayBuffer.empty[ActionStatusResponse[_]]

    context.actorSelection("/user/actions/*") ! GetStatus()
    context.system.scheduler.scheduleOnce(1 second, self, TimedOut)

    val handle = expect {
      case ar: ActionStatusResponse[_] => values += ar
      case TimedOut => processFinal(values.toList)
    }

    def processFinal(eval: List[ActionStatusResponse[_]]) {
      unexpect(handle)
      originalSender ! ProcessList(eval, queues)
      context.stop(self)
    }
  }
}

class ActionsManagerActor() extends Actor {
  import context._
  val log = Logging(system, ActionsManagerActor.this)
  val settings = Settings.get(system)

  val availableTransformations = settings.availableTransformations.keySet()

  val queues = availableTransformations.foldLeft(Map[String, collection.mutable.Queue[CommandWithSender]]()) {
    (registeredDriverQueues, driverName) => registeredDriverQueues + (driverName -> new collection.mutable.Queue[CommandWithSender]())
  }

  private def formatQueues() = {
    queues.map(q => (q._1, q._2.map(c => {
      if (c.command.isInstanceOf[Transformation])
        c.command.asInstanceOf[Transformation].description
      else
        "deploy"
    }).toList))
  }

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _: DriverException => Restart
      case _: Throwable => Escalate
    }

  override def preStart {
    for (transformation <- availableTransformations; _ <- 0 until settings.getDriverSettings(transformation).concurrency) {
      actorOf(DriverActor.props(transformation, self))
    }
  }

  def receive = LoggingReceive({
    case GetStatus() => actorOf(Props[ActionStatusRetriever]) ! GetProcessList(sender(), formatQueues())

    case PollCommand(transformationType) => {
      val queueForType = queues.get(transformationType).get

      if (!queueForType.isEmpty) {
        val cmd = queueForType.dequeue()
        
        sender ! cmd

        if (cmd.command.isInstanceOf[Transformation]) {
          val transformation = cmd.command.asInstanceOf[Transformation]
          log.debug(s"Dequeued ${transformationType} transformation ${transformation}${if (transformation.view.isDefined) s" for view ${transformation.view.get}" else ""}; queue size is now: ${queues.get(transformationType).size}")
        }
        else
          log.debug("Dequeued deploy action")
      }
    }

    case actionCommand: CommandWithSender => {
      if (actionCommand.command.isInstanceOf[Transformation]) {
        val transformation = actionCommand.command.asInstanceOf[Transformation]
        val queueName = transformation.name
  
        queues.get(queueName).get.enqueue(actionCommand)
  
        log.debug(s"Enqueued ${queueName} transformation ${transformation}${if (transformation.view.isDefined) s" for view ${transformation.view.get}" else ""}; queue size is now: ${queues.get(queueName).size}")
      } else {
        queues.values.foreach { _.enqueue(actionCommand) }
      }
        
    }

    case viewAction: View => self ! CommandWithSender(viewAction.transformation().forView(viewAction), sender)

    case filesystemTransformationAction: FilesystemTransformation => self ! CommandWithSender(filesystemTransformationAction, sender)

    case deployAction: Deploy => self ! CommandWithSender(deployAction, sender)
  })
}

object ActionsManagerActor {
  def props(conf: Configuration) = Props[ActionsManagerActor]
}