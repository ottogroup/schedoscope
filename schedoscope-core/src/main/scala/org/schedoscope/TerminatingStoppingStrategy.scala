package org.schedoscope

import akka.actor.SupervisorStrategyConfigurator
import akka.actor.AllForOneStrategy
import akka.actor.SupervisorStrategy.Escalate

class TerminatingStoppingStrategy extends SupervisorStrategyConfigurator{
  override def create = AllForOneStrategy() {
      case t: Throwable => {
        t.printStackTrace()
        Escalate
      }
    }
}