package com.ottogroup.bi.soda.bottler

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt

import com.ottogroup.bi.soda.bottler.api.SettingsImpl
import com.ottogroup.bi.soda.dsl.View

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.OneForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy.Escalate
import akka.actor.actorRef2Scala
import akka.contrib.pattern.Aggregator

class ViewStatusRetriever() extends Actor with Aggregator {
  expectOnce {
    case GetViewStatusList(statusRequester, viewActors) => new MultipleResponseHandler(statusRequester, viewActors)
  }

  class MultipleResponseHandler(statusRequester: ActorRef, viewActors: Seq[ActorRef]) {
    import context.dispatcher
    import collection.mutable.ArrayBuffer

    val values = ArrayBuffer.empty[ViewStatusResponse]

    viewActors.foreach(_ ! GetStatus())
    context.system.scheduler.scheduleOnce(1 second, self, "timeout")

    val handle = expect {
      case viewStatus: ViewStatusResponse => values += viewStatus
      case "timeout" => processFinal(values.toList)
    }

    def processFinal(viewStatus: List[ViewStatusResponse]) {
      unexpect(handle)
      statusRequester ! ViewStatusListResponse(viewStatus)
      context.stop(self)
    }
  }
}

class ViewManagerActor(settings: SettingsImpl, actionsManagerActor: ActorRef, schemaActor: ActorRef) extends Actor {
  import context._

  override def preRestart(reason: Throwable, message: Option[Any]) {
    // prevent termination of children durin restart and cause their own restart
  }

  def receive = {
    case GetStatus() => {
      println("Fetching status from " + children.toList.mkString(","))
      actorOf(Props[ViewStatusRetriever]) ! GetViewStatusList(sender, children.toList)      
    }

    case v: View => {
      //generate a unique id for every actor
      val actorName = v.module + v.n + v.parameters.foldLeft("") { (s, p) => s"${s}+${p.n}=${p.v.get}" }

      val actor = actorFor(actorName)
      sender ! (if (actor.isTerminated)
        actorOf(ViewActor.props(v, settings, self, actionsManagerActor, schemaActor), actorName)
      else
        actor)
    }
  }
}

object ViewManagerActor {
  def props(settings: SettingsImpl, actionsManagerActor: ActorRef, schemaActor: ActorRef): Props = Props(classOf[ViewManagerActor], settings: SettingsImpl, actionsManagerActor, schemaActor)
}