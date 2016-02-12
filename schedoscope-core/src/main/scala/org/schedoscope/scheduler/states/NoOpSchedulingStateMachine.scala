package org.schedoscope.scheduler.states

import java.util.Date

/**
 * This class implements a ViewSchedulingStateMachine for views with NoOp transformations.
 */
class NoOpViewSchedulingStateMachine extends ViewSchedulingStateMachine {

  def materialize(
    currentState: ViewSchedulingState,
    listener: ViewSchedulingStateListener,
    successFlagExists: => Boolean,
    currentTime: Long = new Date().getTime) = currentState match {

    case CreatedByViewManager(view) => {
      if (successFlagExists)
        NewState(
          Materialized(view, view.transformation().checksum, currentTime),
          Set(
            WriteTransformationTimestamp(view, currentTime),
            WriteTransformationCheckum(view),
            ReportMaterialized(view, Set(listener), currentTime, false, false)))
      else
        NewState(
          NoData(view, view.transformation().checksum),
          Set(ReportNoDataAvailable(view, Set(listener))))

    }
  }
}