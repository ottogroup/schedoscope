package org.schedoscope.scheduler.actors

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorInitializationException, ActorKilledException, OneForOneStrategy, Props}
import akka.event.{Logging, LoggingReceive}
import org.schedoscope.conf.SchedoscopeSettings
import org.schedoscope.scheduler.messages.{CollectViewSchedulingStatus, ViewSchedulingListenersExist, ViewSchedulingNewEvent}
import org.schedoscope.scheduler.states.{ViewSchedulingListenerHandler, ViewSchedulingListenerHandlerInternalException}
import org.schedoscope.dsl.View

class ViewSchedulingListenerManagerActor(settings: SchedoscopeSettings) extends Actor {

  import context._

  val log = Logging(system, ViewSchedulingListenerManagerActor.this)

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = -1) {
      case _: ActorInitializationException => Restart
      case _: ActorKilledException => Restart
      case _: ViewSchedulingListenerHandlerInternalException => Restart
      case _ => Escalate
    }

  var viewsMonitored = scala.collection.mutable.Map[View, ViewSchedulingNewEvent]()

  def viewSchedulingListenerHandlers(handlers:List[String] = settings.viewSchedulingRunCompletionHandlers) = {
    if (handlers.length > 0) {

      val handlersSetSoFar = scala.collection.mutable.Set[String]()
      handlers.foreach { x =>
        if (getViewSchedulingHandlerClass(x))
          handlersSetSoFar += x
      }
      handlersSetSoFar
    } else {
      Set[String]()
    }
  }

  def getViewSchedulingHandlerClass(className: String):Boolean = try {
    val c = Class.forName(className).newInstance().asInstanceOf[ViewSchedulingListenerHandler]
    true
  } catch {
    case ex: ClassCastException =>
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

  def receive: Receive = LoggingReceive({

    case ViewSchedulingNewEvent(view, eventTime, action, prevState, newState) => {
      // forward to all its children (aka handlers)
      context.actorSelection("*") ! ViewSchedulingNewEvent(view, eventTime, action, prevState, newState)
      viewsMonitored += (view -> ViewSchedulingNewEvent(view, eventTime, action, prevState, newState))
    }

    case CollectViewSchedulingStatus() =>
      viewsMonitored.values.foreach(sender ! _)

    case ViewSchedulingListenersExist(_) =>
      sender ! ViewSchedulingListenersExist(
        viewSchedulingListenerHandlers().size > 0)
  })

}

/**
  * Factory for ViewSchedulingListenerManager actor.
  */
object ViewSchedulingListenerManagerActor {
  def props(settings: SchedoscopeSettings) =
    Props(classOf[ViewSchedulingListenerManagerActor], settings)
      .withDispatcher("akka.actor.view-scheduling-listener-dispatcher")
}