package com.ottogroup.bi.soda.bottler

import com.ottogroup.bi.soda.bottler.api.SettingsImpl
import com.ottogroup.bi.soda.dsl.View

import akka.actor.Actor
import akka.actor.OneForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy.Escalate
import akka.actor.actorRef2Scala

class ViewManagerActor(settings: SettingsImpl) extends Actor {
  import context._

  override val supervisorStrategy = OneForOneStrategy() {
      case _: Throwable => Escalate
    }

  def receive = {
    case v: View => {
      //generate a unique id for every actor
      val actorName = v.module + v.n + v.parameters.foldLeft("") { (s, p) => s"${s}+${p.n}=${p.v.get}" }

      val actor = actorFor(actorName)
      sender ! (if (actor.isTerminated)
        actorOf(ViewActor.props(v, settings), actorName)
      else
        actor)
    }
  }
}

object ViewManagerActor {
  def props(settings: SettingsImpl): Props = Props(classOf[ViewManagerActor], settings: SettingsImpl)
}