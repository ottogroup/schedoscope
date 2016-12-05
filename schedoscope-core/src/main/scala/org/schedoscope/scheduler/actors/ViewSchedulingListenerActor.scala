package org.schedoscope.scheduler.actors

import akka.actor.{Actor, ActorKilledException, ActorRef, Props}
import akka.event.Logging
import org.joda.time.LocalDateTime
import org.schedoscope.scheduler.states.{ViewSchedulingAction, ViewSchedulingListener, ViewSchedulingListenerHandle, ViewSchedulingListenerHandlerInternalException}
import org.schedoscope.dsl.View
import org.schedoscope.scheduler.messages.{CollectViewSchedulingStatus, ViewSchedulingNewEvent}

/**
  * Mute Actor that just serves to encapsulate in Akka
  * external View Monitoring Handlers
  *
  */

class ViewSchedulingListenerActor(handlerClassName:String,
                                  viewSchedulingListenerManagerActor: ActorRef) extends Actor {

  import context._
  val log = Logging(system, ViewSchedulingListenerActor.this)
  val viewSchedulingListenerHandler = new ViewSchedulingListener(handlerClassName)

  def receive: Receive = {
    case ViewSchedulingNewEvent(view, eventTime, action, prevState, newState) =>
      try {
        viewSchedulingListenerHandler.viewSchedulingCall(
          ViewSchedulingListenerHandle(view, eventTime, new LocalDateTime(), action, prevState, newState))
        logStateInfo(view, prevState, newState, action)
      } catch {
        case e: Throwable =>
          log.error(s"External Handler raised exception: ${e.getMessage}")
          throw new ViewSchedulingListenerHandlerInternalException(e.getMessage)
      }
  }

  override def postRestart(reason: Throwable) {
    super.postRestart(reason)
    log.info(s"Restarted because of ${reason.getMessage}.")
    viewSchedulingListenerManagerActor ! CollectViewSchedulingStatus
  }


  def logStateInfo(view:View, prevState: Option[String], newState:String, action:Option[ViewSchedulingAction]) {
    action match {
      case Some(a) => log.info(s"view: ${View}, prevState: ${prevState}, newState: ${newState}, action: ${a}")
      case None => prevState match {
        case Some(s) => log.info(s"view: ${View}, prevState: ${prevState}, newState: ${newState}")
        case None => log.info(s"Initialized view: ${View} ==>State: ${newState}")
      }
    }
  }

}

/**
  * Factory for ViewSchedulingListenerActor actor.
  */
object ViewSchedulingListenerActor {
  def props(handlerClassName:String, viewSchedulingListenerManagerActor: ActorRef) =
    Props(classOf[ViewSchedulingListenerActor],
      handlerClassName, viewSchedulingListenerManagerActor)
      .withDispatcher("akka.actor.view-scheduling-listener-dispatcher")
}
