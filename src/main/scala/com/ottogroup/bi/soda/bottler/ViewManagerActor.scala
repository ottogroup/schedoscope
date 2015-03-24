package com.ottogroup.bi.soda.bottler

import scala.collection.mutable.ListBuffer

import com.ottogroup.bi.soda.SettingsImpl
import com.ottogroup.bi.soda.bottler.SodaRootActor.settings
import com.ottogroup.bi.soda.dsl.View

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.contrib.pattern.Aggregator

class ViewStatusRetriever() extends Actor with Aggregator {
  expectOnce {
    case GetViewStatusList(statusRequester, viewActors) => if (viewActors.isEmpty) {
      statusRequester ! ViewStatusListResponse(List())
      context.stop(self)
    } else
      new MultipleResponseHandler(statusRequester, viewActors)
  }

  class MultipleResponseHandler(statusRequester: ActorRef, viewActors: Seq[ActorRef]) {
    import context.dispatcher
    import collection.mutable.ArrayBuffer

    val values = ListBuffer[ViewStatusResponse]()

    viewActors.foreach(_ ! GetStatus())
    context.system.scheduler.scheduleOnce(settings.statusListAggregationTimeout, self, "timeout")

    val handle = expect {

      case viewStatus: ViewStatusResponse => {
        values += viewStatus

        if (values.size == viewActors.size)
          processFinal(values.toList)
      }

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
    // prevent termination of children during restart and cause their own restart
  }

  def receive = {
    case GetStatus() => actorOf(Props[ViewStatusRetriever]) ! GetViewStatusList(sender, children.toList)

    case NewDataAvailable(view) => children.filter { _ != sender }.foreach { _ ! NewDataAvailable(view) }

    case v: View => {
      //generate a unique id for every actor
      val actor = ViewManagerActor.actorForView(v)

      sender ! (if (actor.isTerminated)
        actorOf(ViewActor.props(v, settings, self, actionsManagerActor, schemaActor), ViewManagerActor.actorNameForView(v))
      else
        actor)
    }
  }
}

object ViewManagerActor {
  def props(settings: SettingsImpl, actionsManagerActor: ActorRef, schemaActor: ActorRef): Props = Props(classOf[ViewManagerActor], settings: SettingsImpl, actionsManagerActor, schemaActor)

  def actorNameForView(v: View) = v.module + v.n + v.parameters.foldLeft("") { (s, p) => s"${s}+${p.n}=${p.v.get}" }

  def actorForView(v: View) = SodaRootActor.settings.system.actorFor(SodaRootActor.viewManagerActor.path.child(actorNameForView(v)))

}