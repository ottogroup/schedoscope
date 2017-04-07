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

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorInitializationException, ActorKilledException, ActorRef, OneForOneStrategy, Props}
import akka.event.{Logging, LoggingReceive}
import org.schedoscope.conf.SchedoscopeSettings
import org.schedoscope.dsl.View
import org.schedoscope.scheduler.listeners.{RetryableViewSchedulingListenerException, ViewSchedulingListener, ViewSchedulingListenerException}
import org.schedoscope.scheduler.messages.{CollectViewSchedulingStatus, RegisterFailedListener, ViewSchedulingMonitoringEvent}

/**
  * The view scheduling listeners manager actor is the supervisor of listener classes implemented
  * with the purpose of monitoring the state evolution of scheduling operations (such as
  * materializing, invalidating, etc) upon views. It is started by default along the schedoscope
  * actor system, and will instantiate on initialization one view scheduling listener actor per
  * listener class implemented from org.schedoscope.scheduler.listeners.ViewSchedulingListener.
  *
  * The supervision strategy implemented for the listeners is designed so that only if the listener
  * class throws a RetryableViewSchedulingListenerException will the restarting listener actor send
  * a Message to the manager to register to receive the latest event for each View (before restart),
  * and another post restart to collect the respective events. Otherwise, upon other exceptions the
  * actor will simply be restarted.
  *
  * Finally the maximum number of retries is configurable via schedoscope.conf. If no value is specified,
  * then default param is -1 (infinite).
  */
class ViewSchedulingListenerManagerActor(settings: SchedoscopeSettings) extends Actor {

  import context._

  val log = Logging(system, ViewSchedulingListenerManagerActor.this)

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = settings.viewScheduleListenerActorsMaxRetries) {
      case _: ActorInitializationException => Restart
      case _: ActorKilledException => Restart
      case _: RetryableViewSchedulingListenerException => Restart
      case _: ViewSchedulingListenerException => Restart
      case _ => Escalate
    }

  var viewsMonitored = Map[View, ViewSchedulingMonitoringEvent]()
  var handlersMonitored = Set[String]()

  def viewSchedulingListenerHandlers(handlers: List[String] = settings.viewSchedulingListeners) =
    handlers.filter(getViewSchedulingHandlerClass).toSet

  def getViewSchedulingHandlerClass(className: String): Boolean =
    try {
      Class.forName(className).newInstance().asInstanceOf[ViewSchedulingListener]
      true
    } catch {
      case _: ClassNotFoundException =>
        val msg = s"Class ${className} was not found. Skipping listener creation."
        log.error(msg)
        false
    }

  /**
    * Create viewSchedulingListener handler actors ONLY if there are any
    * Monitoring classes.
    */
  override def preStart {
    viewSchedulingListenerHandlers().foreach(handler =>
      actorOf(
        ViewSchedulingListenerActor.props(
          handler, self), handler)
    )
  }

  override def postRestart(reason: Throwable) {
    super.postRestart(reason)
    log.info(s"Restarted because of ${reason.getMessage}")
  }

  def collectViewSchedulingStatus(actor: ActorRef, handlerClassName: String) =
    if (handlersMonitored contains handlerClassName) {
      handlersMonitored -= handlerClassName
      viewsMonitored.values.foreach(actor ! _)
    }

  def receive: Receive = LoggingReceive({

    case ViewSchedulingMonitoringEvent(prevState, newState, actions, eventTime) =>
      context.actorSelection(s"${self.path}/*") ! ViewSchedulingMonitoringEvent(prevState, newState, actions, eventTime)
      viewsMonitored += (prevState.view -> ViewSchedulingMonitoringEvent(prevState, newState, actions, eventTime))

    case CollectViewSchedulingStatus(handlerClassName) =>
      collectViewSchedulingStatus(sender, handlerClassName)


    case RegisterFailedListener(handlerClassName) =>
      handlersMonitored += handlerClassName

  })

}

/**
  * Factory for ViewSchedulingListenerManager actor.
  */
object ViewSchedulingListenerManagerActor {
  def props(settings: SchedoscopeSettings) =
    Props(classOf[ViewSchedulingListenerManagerActor], settings)
}