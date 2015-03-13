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

    context.actorSelection("/user/soda/actions/*") ! GetStatus()
    context.system.scheduler.scheduleOnce(50 milliseconds, self, TimedOut)

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
    case PollCommand(typ) => {
      queues.get(typ).map(q => if (!q.isEmpty) {
        val cmd = q.dequeue()
        val targetView = cmd.message.asInstanceOf[Transformation].getView
        log.debug(s"Sending ${typ} transformation for view ${targetView} to ${sender}; remaining ${typ} jobs: ${queues.get(typ).size}")
        sender ! cmd
      })
    }

    case view: View => {
      val cmd = view.transformation().forView(view)
      queues.get(cmd.name).get.enqueue(CommandWithSender(cmd, sender))
      log.debug(s"Enqueued transformation for view ${view.viewId}; queue size is now: ${queues.get(cmd.name).size}")
    }

    case cmd: FilesystemTransformation => {
      queues.get(FileSystemDriver.name).get.enqueue(CommandWithSender(cmd, sender))
      log.debug(s"Enqueued transformation for ${FileSystemDriver.name}; queue size is now: ${queues.get(FileSystemDriver.name).size}")
    }

    case cmd: GetStatus => actorOf(Props[ActionStatusRetriever]) ! GetProcessList(sender(), formatQueues())

    case cmd: Deploy => queues.values.foreach { _.enqueue(CommandWithSender(cmd, sender)) }
  })

  private def formatQueues() = {
    queues.map(q => (q._1, q._2.map(c => {
      c.message.asInstanceOf[Transformation].description
    }).toList))
  }
}

object ActionsManagerActor {
  def props(conf: Configuration) = Props[ActionsManagerActor]
}