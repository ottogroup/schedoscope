package org.schedoscope.scheduler.states

import java.util.Date
import org.schedoscope.scheduler.messages.MaterializeViewMode._

/**
 * This class implements a ViewSchedulingStateMachine for views with NoOp transformations.
 */
class NoOpLeafViewSchedulingStateMachine extends ViewSchedulingStateMachine {

  def materialize(
    currentState: ViewSchedulingState,
    listener: PartyInterestedInViewSchedulingStateChange,
    successFlagExists: => Boolean,
    materializationMode: MaterializeViewMode = DEFAULT,
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
          NoData(view),
          Set(ReportNoDataAvailable(view, Set(listener))))
    }

    case ReadFromSchemaManager(view, checksum, lastTransformationTimestamp) => {
      ResultingViewSchedulingState(
        Materialized(view, checksum, lastTransformationTimestamp),
        Set(
          ReportMaterialized(view, Set(listener), lastTransformationTimestamp, false, false)))
    }

    case Invalidated(view) => {
      if (successFlagExists)
        ResultingViewSchedulingState(
          Materialized(view, view.transformation().checksum, currentTime),
          Set(
            WriteTransformationTimestamp(view, currentTime),
            WriteTransformationCheckum(view),
            ReportMaterialized(view, Set(listener), currentTime, false, false)))
      else
        ResultingViewSchedulingState(
          NoData(view),
          Set(ReportNoDataAvailable(view, Set(listener))))
    }

    case NoData(view) => {
      if (successFlagExists)
        ResultingViewSchedulingState(
          Materialized(view, view.transformation().checksum, currentTime),
          Set(
            WriteTransformationTimestamp(view, currentTime),
            WriteTransformationCheckum(view),
            ReportMaterialized(view, Set(listener), currentTime, false, false)))
      else
        ResultingViewSchedulingState(
          NoData(view),
          Set(ReportNoDataAvailable(view, Set(listener))))
    }

    case Materialized(view, checksum, lastTransformationTimestamp) => {
      ResultingViewSchedulingState(
        Materialized(view, checksum, lastTransformationTimestamp),
        Set(
          ReportMaterialized(view, Set(listener), lastTransformationTimestamp, false, false)))
    }
  }
}