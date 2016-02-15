package org.schedoscope.scheduler.states

import java.util.Date

/**
 * This class implements a ViewSchedulingStateMachine for views with NoOp transformations.
 */
class NoOpViewSchedulingStateMachine extends ViewSchedulingStateMachine {

  def materialize(
    currentState: ViewSchedulingState,
    listener: PartyInterestedInViewSchedulingStateChange,
    successFlagExists: => Boolean,
    currentTime: Long = new Date().getTime) = currentState match {

    case CreatedByViewManager(view) => {
      if (successFlagExists)
        ResultingViewSchedulingState(
          Materialized(view, view.transformation().checksum, currentTime),
          Set(
            WriteTransformationTimestamp(view, currentTime),
            WriteTransformationCheckum(view),
            ReportMaterialized(view, Set(listener), currentTime, false, false)))
      else
        ResultingViewSchedulingState(
          NoData(view, view.transformation().checksum),
          Set(ReportNoDataAvailable(view, Set(listener))))

    }

    case Materialized(view, checksum, lastTransformationTimestamp) => {
      ResultingViewSchedulingState(
        Materialized(view, checksum, lastTransformationTimestamp),
        Set(
          ReportMaterialized(view, Set(listener), lastTransformationTimestamp, false, false)))
    }
    
    case NoData(view, checksum) => {
      if (successFlagExists)
        ResultingViewSchedulingState(
          Materialized(view, checksum, currentTime),
          Set(
            WriteTransformationTimestamp(view, currentTime),
            WriteTransformationCheckum(view),
            ReportMaterialized(view, Set(listener), currentTime, false, false)))
      else
        ResultingViewSchedulingState(
          NoData(view, checksum),
          Set(ReportNoDataAvailable(view, Set(listener))))

    }
  }
}