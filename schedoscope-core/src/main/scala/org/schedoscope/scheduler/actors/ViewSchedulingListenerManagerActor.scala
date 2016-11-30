package org.schedoscope.scheduler.actors

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorInitializationException, OneForOneStrategy, Props}
import akka.event.{Logging, LoggingReceive}
import org.schedoscope.conf.SchedoscopeSettings

class ViewSchedulingListenerManagerActor(settings: SchedoscopeSettings) extends Actor {

  import context._
  val log = Logging(system, ViewSchedulingListenerManagerActor.this)

  override val supervisorStrategy = (
    OneForOneStrategy(maxNrOfRetries = -1) {
      case _: ActorInitializationException => Restart
      case _ => Escalate
    })


  /**
    * Create viewScheduling handler actors as required by configured scheduler handler types.
    */
  override def preStart {
    for (c <- 0 until settings.viewSchedulingRunCompletionHandlers.length) {
      actorOf(ViewSchedulingListenerActor.props(, self), s"${settings.viewSchedulingRunCompletionHandlers(c)}-${c + 1}")
    }
  }

  def receive: Receive = LoggingReceive({

  })

}

/**
  * Factory for ViewSchedulingListenerManager actor.
  */
object ViewSchedulingListenerManagerActor {
  def props(settings: SchedoscopeSettings) = Props(classOf[ViewSchedulingListenerManagerActor], settings).withDispatcher("akka.actor.view-stage-change-listener-dispatcher")
}