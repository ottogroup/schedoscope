package org.schedoscope

import akka.actor.SupervisorStrategyConfigurator
import akka.actor.AllForOneStrategy
import akka.actor.SupervisorStrategy.Escalate
import org.slf4j.LoggerFactory

class TerminatingStoppingStrategy extends SupervisorStrategyConfigurator {
  override def create = AllForOneStrategy() {
      case t: Throwable => {
        val log = LoggerFactory.getLogger(classOf[TerminatingStoppingStrategy])
        
        log.error("Terminating Schedoscope: unhandled exception got escalated up the supervisor hierarchy.", t)
        
        System.err.println("Terminating Schedoscope: unhandled exception got escalated up the supervisor hierarchy.")
        t.printStackTrace()
        
        System.exit(1)
        Escalate
      }
    }
}