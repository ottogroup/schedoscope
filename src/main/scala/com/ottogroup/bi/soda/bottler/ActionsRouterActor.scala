package com.ottogroup.bi.soda.bottler

import akka.actor.Actor
import akka.actor.Props
import akka.routing.BroadcastRouter
import akka.actor.ActorRef
import com.ottogroup.bi.soda.dsl.transformations.sql.HiveQl
import com.ottogroup.bi.soda.dsl.transformations.filesystem.FileOperation
import org.apache.hadoop.conf.Configuration
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration.Duration
import akka.util.Timeout
import akka.event.LoggingReceive
import akka.event.Logging
import com.ottogroup.bi.soda.dsl.transformations.oozie.OozieWF
import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.dsl.transformations.filesystem.CopyFrom
import com.ottogroup.bi.soda.dsl.transformations.filesystem.Copy
import akka.contrib.pattern.Aggregator
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

case class PollCommand(typ: String)
case class CommandWithSender(message: AnyRef, sender: ActorRef)
case class WorkAvailable()
case class TimedOut
case class ProcessList(status: List[ActionStatusResponse])
case class GetProcessList(sender:ActorRef)
class StatusRetriever extends Actor with Aggregator {

  expectOnce {
    case GetProcessList(s) ⇒ new MultipleResponseHandler(s, "")
  }

  class MultipleResponseHandler(originalSender: ActorRef, propName: String) {

    import context.dispatcher
    import collection.mutable.ArrayBuffer

    val values = ArrayBuffer.empty[ActionStatusResponse]

    context.actorSelection("/user/actions/*") ! GetStatus()
    context.system.scheduler.scheduleOnce(50 milliseconds, self, TimedOut)

    val handle = expect {
      case ar: ActionStatusResponse ⇒
        values += ar
      case TimedOut ⇒ processFinal(values.toList)
    }

    def processFinal(eval: List[ActionStatusResponse]) {
      unexpect(handle)
      originalSender ! ProcessList(eval)
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
class ActionsRouterActor(conf: Configuration) extends Actor {
  import context._
  val log = Logging(system, this)
  val url = "http://anatolefrance:11000/oozie"
  val jdbcUrl = "jdbc:hive2://anatolefrance:10000/default;principal=hive/anatolefrance@OTTOGROUP.COM"
  val queues = Map("hive" -> new collection.mutable.Queue[CommandWithSender],
    "oozie" -> new collection.mutable.Queue[CommandWithSender],
    "file" -> new collection.mutable.Queue[CommandWithSender])

  val routers = Map("oozie" -> actorOf(OozieActor.props(url).withRouter(
    BroadcastRouter(nrOfInstances = 50))),
    "hive" -> actorOf(HiveActor.props(jdbcUrl).withRouter(
      BroadcastRouter(nrOfInstances = 50))),
    "file" -> actorOf(FileSystemActor.props(conf).withRouter(
      BroadcastRouter(nrOfInstances = 1))))

  def receive = LoggingReceive({
    case PollCommand(typ) =>
      queues.get(typ).map(q => if (!q.isEmpty) sender ! q.dequeue)
    case view: View => view.transformation() match {
      case cmd: OozieWF => {
        queues.get("oozie").get.enqueue(CommandWithSender(cmd, sender))
        routers.get("oozie").get ! WorkAvailable
      }
      case cmd: HiveQl => {
        queues.get("hive").get.enqueue(CommandWithSender(cmd, sender))
        routers.get("hive").get ! WorkAvailable
      }
      case cmd: CopyFrom => routers.get("file").get ! CommandWithSender(Copy(cmd.fromPattern, view.partitionPathBuilder()), sender)
      case cmd: FileOperation  => {
        //    queues.get("file").get.enqueue(CommandWithSender(cmd, sender))
        routers.get("file").get ! CommandWithSender(cmd, sender)
      }
    }
     case cmd: OozieWF => {
        queues.get("oozie").get.enqueue(CommandWithSender(cmd, sender))
        routers.get("oozie").get ! WorkAvailable
      }
      case cmd: HiveQl => {
        queues.get("hive").get.enqueue(CommandWithSender(cmd, sender))
        routers.get("hive").get ! WorkAvailable
      }
     
      case cmd: FileOperation  => {
        //    queues.get("file").get.enqueue(CommandWithSender(cmd, sender))
        routers.get("file").get ! CommandWithSender(cmd, sender)
      }
    case cmd: GetStatus => {
      implicit val timeout = Timeout(600);
      actorOf(Props(new StatusRetriever)) ! GetProcessList(sender())
//      routers.map { case (name, router) => router ! CommandWithSender(cmd, sender) }.map { x => x }
    }
   
  })
}

object ActionsRouterActor {
  def props(conf: Configuration) = Props(new ActionsRouterActor(conf))
}