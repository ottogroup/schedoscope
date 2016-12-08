package org.schedoscope.scheduler.actors

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorInitializationException, ActorKilledException, ActorRef, OneForOneStrategy, Props}
import akka.event.{Logging, LoggingReceive}
import org.schedoscope.conf.SchedoscopeSettings
import org.schedoscope.scheduler.messages.{CollectViewSchedulingStatus, RegisterFailedListener, ViewSchedulingMonitoringEvent}
import org.schedoscope.dsl.View
import org.schedoscope.scheduler.listeners.{RetryableViewSchedulingListenerException, ViewSchedulingListener, ViewSchedulingListenerException}

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

  var viewsMonitored = scala.collection.mutable.Map[View, ViewSchedulingMonitoringEvent]()
  var handlersMonitored = scala.collection.mutable.Set[String]()

  def viewSchedulingListenerHandlers(handlers:List[String] = settings.viewSchedulingRunCompletionHandlers) = {
    val handlersSetSoFar = scala.collection.mutable.Set[String]()
    handlers.foreach { x =>
      if (getViewSchedulingHandlerClass(x))
        handlersSetSoFar += x
    }
    handlersSetSoFar
  }

  def getViewSchedulingHandlerClass(className: String):Boolean = try {
    val c = Class.forName(className).newInstance().asInstanceOf[ViewSchedulingListener]
    true
  } catch {
    case ex: ClassCastException  =>
      log.error(ex.getMessage)
      false
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
    viewSchedulingListenerHandlers().toSeq.foreach( handler =>
      actorOf(
        ViewSchedulingListenerActor.props(
          handler, self), handler)
    )
  }

  override def postRestart(reason: Throwable) {
    super.postRestart(reason)
    log.info(s"Restarted because of ${reason.getMessage}")
  }

  def collectViewSchedulingStatus(actor:ActorRef, handlerClassName:String) =
    if(handlersMonitored contains handlerClassName) {
      handlersMonitored -= handlerClassName
      viewsMonitored.values.foreach(actor ! _)
    }

  def receive: Receive = LoggingReceive({

    case ViewSchedulingMonitoringEvent(prevState, newState, actions, eventTime) => {
      // forward to all its children (aka handlers)
      context.actorSelection("*") ! ViewSchedulingMonitoringEvent(prevState, newState, actions, eventTime)
      viewsMonitored += (prevState.view -> ViewSchedulingMonitoringEvent(prevState, newState, actions, eventTime))
    }

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