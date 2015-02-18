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
import com.ottogroup.bi.soda.bottler.api.Settings
import collection.JavaConversions._
import com.typesafe.config.Config
import com.ottogroup.bi.soda.bottler.api.Settings
import com.ottogroup.bi.soda.bottler.api.SettingsImpl
import com.typesafe.config.ConfigObject
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValueFactory
import com.ottogroup.bi.soda.dsl.Transformation

class StatusRetriever extends Actor with Aggregator {

  expectOnce {
    case GetProcessList(s) => new MultipleResponseHandler(s, "")
  }

  class MultipleResponseHandler(originalSender: ActorRef, propName: String) {

    import context.dispatcher
    import collection.mutable.ArrayBuffer

    val values = ArrayBuffer.empty[ActionStatusResponse]

    context.actorSelection("/user/actions/*") ! GetStatus()
    context.system.scheduler.scheduleOnce(50 milliseconds, self, TimedOut)

    val handle = expect {
      case ar: ActionStatusResponse => values += ar
      case TimedOut => processFinal(values.toList)
    }

    def processFinal(eval: List[ActionStatusResponse]) {
      unexpect(handle)
      originalSender ! ProcessList(eval)
      context.stop(self)
    }
  }
}

object ActionFactory {
  def createActor(name: String, config: Config, settings: SettingsImpl) = {
    println(config)
    name match {
      case "hive" => HiveActor.props(config)
      case "oozie" => OozieActor.props(config)
      case "file" => FileSystemActor.props(settings)
    }
  }
  def getTransformationTypeName(t:Transformation) =
    t match {
    	case _:OozieWF => "oozie"
    	case _:HiveQl=>"hive"
    	case _:FileOperation => "file" 
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
  val settings = Settings.get(system)

  val queues =
    settings.availableTransformations.entrySet().foldLeft(Map[String, collection.mutable.Queue[CommandWithSender]]()) {
    (map, entry) =>{
      map + (entry.getKey() ->      
       new collection.mutable.Queue[CommandWithSender]())
      
    }
  }
  val routers = settings.availableTransformations.entrySet().foldLeft(Map[String, ActorRef]()) {
    (map, entry) =>{
        val conf = entry.getValue().asInstanceOf[ConfigObject].toConfig().withFallback(ConfigFactory.empty.withValue("concurrency", ConfigValueFactory.fromAnyRef(1)))
      map + (entry.getKey() ->      
        actorOf(ActionFactory.createActor(entry.getKey(), conf, settings).withRouter(BroadcastRouter(nrOfInstances = conf.getInt("concurrency")))))
      
    }
  }

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
      case cmd: FileOperation => {
        queues.get("file").get.enqueue(CommandWithSender(cmd, sender))
        routers.get("file").get ! WorkAvailable
      //  CommandWithSender(cmd, sender)
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

    case cmd: FileOperation => {
      routers.get("file").get ! CommandWithSender(cmd, sender)
    }
    case cmd: GetStatus => {
      implicit val timeout = Timeout(600);
      actorOf(Props(new StatusRetriever)) ! GetProcessList(sender())
    }
    case cmd: Deploy => {
      
    }
  })
}

object ActionsRouterActor {
  def props(conf: Configuration) = Props(new ActionsRouterActor(conf))
}