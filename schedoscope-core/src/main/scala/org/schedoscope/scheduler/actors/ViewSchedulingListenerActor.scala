package org.schedoscope.scheduler.actors

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorInitializationException, OneForOneStrategy, Props}
import akka.event.{Logging, LoggingReceive}
import org.schedoscope.conf.SchedoscopeSettings
import org.schedoscope.dsl.View

class ViewSchedulingListenerActor(view:View) extends Actor {

  import context._
  val log = Logging(system, ViewSchedulingListenerActor.this)


  def receive: Receive = LoggingReceive({

  })

  def logStateInfo(state: String, message: String) {
    //viewStateChangeManagerActor ! ViewStatusResponse(state, self, driver, runHandle, runState)
    log.info(message)
  }

}

/**
  * Factory for ViewSchedulingListenerActor actor.
  */
object ViewSchedulingListenerActor {
  def props(settings: SchedoscopeSettings) = Props(classOf[ViewSchedulingListenerActor], settings).withDispatcher("akka.actor.view-stage-change-listener-dispatcher")
}
