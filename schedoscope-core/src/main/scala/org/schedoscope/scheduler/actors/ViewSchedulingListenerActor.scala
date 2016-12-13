/**
  * Copyright 2015 Otto (GmbH & Co KG)
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
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
