package com.ottogroup.bi.soda.bottler

import akka.actor.Actor
import akka.actor.Props
import akka.routing.BroadcastRouter
import akka.actor.ActorRef
import com.ottogroup.bi.soda.dsl.transformations.sql.HiveTransformation
import com.ottogroup.bi.soda.dsl.transformations.filesystem.FilesystemTransformation
import org.apache.hadoop.conf.Configuration
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration.Duration
import akka.util.Timeout
import akka.event.LoggingReceive
import akka.event.Logging
import com.ottogroup.bi.soda.dsl.transformations.oozie.OozieTransformation
import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.dsl.transformations.filesystem.CopyFrom
import com.ottogroup.bi.soda.dsl.transformations.filesystem.Copy
import akka.contrib.pattern.Aggregator
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import com.ottogroup.bi.soda.bottler.api.Settings
import collection.JavaConversions._
import com.typesafe.config.Config
import com.ottogroup.bi.soda.bottler.api.Settings
import com.ottogroup.bi.soda.bottler.api.SettingsImpl
import com.typesafe.config.ConfigObject
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValueFactory
import com.ottogroup.bi.soda.dsl.Transformation
import com.ottogroup.bi.soda.bottler.api.DriverSettings

/**
 * This actor aggregrates responses from multiple Actors
 * Used for retrieving running jobs,
 *
 * @author dev_hzorn
 *
 */
class StatusRetriever extends Actor with Aggregator {

  expectOnce {
    case GetProcessList(s, q) => new MultipleResponseHandler(s, q, "")
  }

  class MultipleResponseHandler(originalSender: ActorRef, queues: Map[String, List[String]], propName: String) {

    import context.dispatcher
    import collection.mutable.ArrayBuffer

    val values = ArrayBuffer.empty[ActionStatusResponse[_]]

    context.actorSelection("/user/actions/*") ! GetStatus()
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

/**
 * Supervisor for Hive, Oozie, Routers
 * Implements a pull-work-pattern that does not fill the mailboxes of actors.
 * This way, a long running job will not block short-running
 * In future we should learn runtimes of jobs and distribute to dedicated queues.
 */
class ActionsRouterActor() extends Actor {
  import context._
  val log = Logging(system, this)
  val settings = Settings.get(system)

  val availableTransformations = settings.availableTransformations.keySet()

  val queues = availableTransformations.foldLeft(Map[String, collection.mutable.Queue[CommandWithSender]]()) {
    (registeredDriverQueues, driverName) => registeredDriverQueues + (driverName -> new collection.mutable.Queue[CommandWithSender]())
  }

  val routers = availableTransformations.foldLeft(Map[String, ActorRef]()) {
    (registeredDrivers, driverName) => registeredDrivers + (driverName -> actorOf(DriverActor.props(driverName, self)))
  }

  def receive = LoggingReceive({

    case PollCommand(typ) => {
      queues.get(typ).map(q => if (!q.isEmpty) sender ! q.dequeue)
    }

    case view: View => {
      val cmd = view.transformation().forView(view) // backreference transformation -> view
      queues.get(cmd.typ).get.enqueue(CommandWithSender(cmd, sender))
    }

    case cmd: FilesystemTransformation => {
      queues.get("filesystem").get.enqueue(CommandWithSender(cmd, sender))
    }

    case cmd: GetStatus => {
      implicit val timeout = Timeout(600);
      actorOf(Props(new StatusRetriever)) ! GetProcessList(sender(), formatQueues())
    }

    case cmd: Deploy => {
      routers.map(el => {
        val name = el._1
        val act = el._2
        queues.get(name).get.enqueue(CommandWithSender(cmd, sender))
        //act ! WorkAvailable
      })
    }
  })

  private def formatQueues() = {
    queues.map(q => (q._1, q._2.map(c => {
      val viewId = if (c.message.asInstanceOf[Transformation].view.isDefined) c.message.asInstanceOf[Transformation].view.get.viewId else "no-view"
      c.message.asInstanceOf[Transformation].description
    }).toList))
  }
}

object ActionsRouterActor {
  def props(conf: Configuration) = Props(new ActionsRouterActor())
}