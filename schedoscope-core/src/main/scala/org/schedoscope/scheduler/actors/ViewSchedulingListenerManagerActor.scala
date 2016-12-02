package org.schedoscope.scheduler.actors

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorInitializationException, ActorKilledException, OneForOneStrategy, Props}
import akka.event.{Logging, LoggingReceive}
import org.schedoscope.conf.SchedoscopeSettings
import org.schedoscope.scheduler.messages.{CollectViewSchedulingStatus, ViewSchedulingListenersExist, ViewSchedulingNewEvent}
import org.schedoscope.scheduler.states.ViewSchedulingListenerHandler
import org.schedoscope.dsl.View

class ViewSchedulingListenerManagerActor(settings: SchedoscopeSettings) extends Actor {

  import context._

  val log = Logging(system, ViewSchedulingListenerManagerActor.this)

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = -1) {
      case _: ActorInitializationException => Restart
      case _: ActorKilledException => Restart
      case _ => Escalate
    }

  var viewsMonitored = scala.collection.mutable.Map[View, ViewSchedulingNewEvent]()

  lazy val viewSchedulingListenerHandlersAndMonitoredViews =
    if(settings.viewSchedulingRunCompletionHandlers.length > 0) {
      val handlersMapSoFar = scala.collection.mutable.Set[String]()
      settings.viewSchedulingRunCompletionHandlers.foreach { x =>
        if(getViewSchedulingHandlerClass(x))
            handlersMapSoFar + x
      }
      handlersMapSoFar
  } else {
    Set[String]()
  }

  private def getViewSchedulingHandlerClass(className: String):Boolean = try {
    Class.forName(className).newInstance().asInstanceOf[ViewSchedulingListenerHandler]
    true
  } catch {
    case _: ClassNotFoundException =>
      val msg = s"Class ${className} was not found. Skipping handler."
      log.error(msg)
      false
  }

  /**
    * Create viewSchedulingListener handler actors ONLY if there are any
    * Monitoring classes.
    */
  override def preStart {
    viewSchedulingListenerHandlersAndMonitoredViews.toSeq.foreach( handler =>
      actorOf(
        ViewSchedulingListenerActor.props(
          handler, self), handler)
    )
  }

  override def postRestart(reason: Throwable) {
    super.postRestart(reason)
    log.info(s"Restarted because of ${reason.getMessage}")
  }

  def receive: Receive = LoggingReceive({

    case ViewSchedulingNewEvent(view, eventTime, action, prevState, newState) => {
      // forward to all its children (aka handlers)
      context.actorSelection("*")
      viewsMonitored += (view -> ViewSchedulingNewEvent(view, eventTime, action, prevState, newState))
    }

    case CollectViewSchedulingStatus() =>
      viewsMonitored.values.foreach(sender ! _)

    case ViewSchedulingListenersExist(_) =>
      sender ! ViewSchedulingListenersExist(
        viewSchedulingListenerHandlersAndMonitoredViews.size > 0)
  })

}

/**
  * Factory for ViewSchedulingListenerManager actor.
  */
object ViewSchedulingListenerManagerActor {
  def props(settings: SchedoscopeSettings) =
    Props(classOf[ViewSchedulingListenerManagerActor], settings)
      .withDispatcher("akka.actor.view-scheduling-listener-dispatcher ")
}