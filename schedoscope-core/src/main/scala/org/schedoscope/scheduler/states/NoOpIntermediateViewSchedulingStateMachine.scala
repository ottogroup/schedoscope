package org.schedoscope.scheduler.states

import java.util.Date
import org.schedoscope.scheduler.messages.MaterializeViewMode._

class NoOpIntermediateViewSchedulingStateMachine extends ViewSchedulingStateMachine {

  def materialize(
    currentState: ViewSchedulingState,
    listener: PartyInterestedInViewSchedulingStateChange,
    successFlagExists: => Boolean,
    materializationMode: MaterializeViewMode = DEFAULT,
    currentTime: Long = new Date().getTime) = currentState match {
    
    case _ => null
  }
}