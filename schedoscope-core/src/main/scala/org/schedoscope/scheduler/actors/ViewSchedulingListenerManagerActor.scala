package org.schedoscope.scheduler.actors

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorInitializationException, ActorKilledException, OneForOneStrategy, Props}
import akka.event.{Logging, LoggingReceive}
import org.schedoscope.conf.SchedoscopeSettings
import org.schedoscope.dsl.View
import org.schedoscope.scheduler.messages.{ViewSchedulingListenersExist, ViewSchedulingNewEvent}
import org.schedoscope.scheduler.states.{ViewSchedulingAction, ViewSchedulingListenerHandler}


class ViewSchedulingListenerManagerActor(settings: SchedoscopeSettings) extends Actor {

  import context._

  val log = Logging(system, ViewSchedulingListenerManagerActor.this)

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = -1) {
      case _: ActorInitializationException => Restart
      case _: ActorKilledException => Restart
      case _ => Escalate
    }

  lazy val viewSchedulingListenerHandlersAndMonitoredViews = {
    (0 until settings.viewSchedulingRunCompletionHandlers.length).foldLeft(
    Set[String]()) {
      (handlersMapSoFar, n) => {
        getViewSchedulingHandlerClass(settings.viewSchedulingRunCompletionHandlers(n)) match {
          case h: ViewSchedulingListenerHandler =>
            handlersMapSoFar + (settings.viewSchedulingRunCompletionHandlers(n))
        }
      }
    }
  }

  private def getViewSchedulingHandlerClass(className: String) = try {
    Class.forName(className).newInstance().asInstanceOf[ViewSchedulingListenerHandler]
  } catch {
    case _: ClassNotFoundException =>
      val msg = s"Class ${className} was not found. Skipping handler."
      log.error(msg)
  }

  /**
    * Create viewSchedulingListener handler actors ONLY if there are any
    * Monitoring classes.
    */
  override def preStart {
    viewSchedulingListenerHandlersAndMonitoredViews.toSeq.foreach( handler =>
      actorOf(
        ViewSchedulingListenerActor.props(
          handler), handler)
    )
  }

  override def postRestart(reason: Throwable) {
    super.postRestart(reason)
    log.info(s"Restarted because of ${reason.getMessage}")
  }

  def receive: Receive = LoggingReceive({
    case ViewSchedulingNewEvent(view, action, prevState, newState) =>
      println("TODO - forward to all Children")
    case ViewSchedulingListenersExist(_) =>
      sender ! ViewSchedulingListenersExist(viewSchedulingListenerHandlersAndMonitoredViews.size > 0)
  })

}

/**
  * Factory for ViewSchedulingListenerManager actor.
  */
object ViewSchedulingListenerManagerActor {
  def props(settings: SchedoscopeSettings) =
    Props(classOf[ViewSchedulingListenerManagerActor], settings)
      .withDispatcher("akka.actor.view-stage-change-listener-dispatcher")
}