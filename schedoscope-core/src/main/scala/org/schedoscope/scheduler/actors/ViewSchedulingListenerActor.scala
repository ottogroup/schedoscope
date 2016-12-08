package org.schedoscope.scheduler.actors

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import org.schedoscope.scheduler.listeners.{RetryableViewSchedulingListenerException, ViewSchedulingListenerException, ViewSchedulingListenerHandler}
import org.schedoscope.scheduler.states._
import org.schedoscope.scheduler.messages.{CollectViewSchedulingStatus, RegisterFailedListener, ViewSchedulingMonitoringEvent}

/**
  * Mute Actor that just serves to encapsulate in Akka
  * external View Monitoring Handlers
  *
  */

class ViewSchedulingListenerActor(handlerClassName:String,
                                  viewSchedulingListenerManagerActor: ActorRef) extends Actor {

  import context._
  val log = Logging(system, ViewSchedulingListenerActor.this)
  val viewSchedulingListenerHandler = new ViewSchedulingListenerHandler(handlerClassName)


  def receive: Receive = {
    case ViewSchedulingMonitoringEvent(prevState, newState, actions, eventTime) =>
      try {
        viewSchedulingListenerHandler.viewSchedulingCall(
          ViewSchedulingEvent(prevState, newState, actions, eventTime))
      } catch {

        case e: RetryableViewSchedulingListenerException =>
          viewSchedulingListenerManagerActor ! RegisterFailedListener(handlerClassName)
          throw e

        case t: Throwable =>
          throw new ViewSchedulingListenerException(t.getMessage, t.getCause)
      }
  }

  override def postRestart(reason: Throwable) {
    super.postRestart(reason)
    viewSchedulingListenerManagerActor ! CollectViewSchedulingStatus(handlerClassName)
  }

}

/**
  * Factory for ViewSchedulingListenerActor actor.
  */
object ViewSchedulingListenerActor {
  def props(handlerClassName:String, viewSchedulingListenerManagerActor: ActorRef) =
    Props(classOf[ViewSchedulingListenerActor],
      handlerClassName, viewSchedulingListenerManagerActor)
}
