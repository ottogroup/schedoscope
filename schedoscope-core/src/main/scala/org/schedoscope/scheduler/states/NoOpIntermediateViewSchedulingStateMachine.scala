package org.schedoscope.scheduler.states

import java.util.Date
import org.schedoscope.scheduler.messages.MaterializeViewMode._
import org.schedoscope.dsl.transformations.Checksum.defaultDigest

class NoOpIntermediateViewSchedulingStateMachine extends ViewSchedulingStateMachine {

  def materialize(
    currentState: ViewSchedulingState,
    listener: PartyInterestedInViewSchedulingStateChange,
    successFlagExists: => Boolean,
    materializationMode: MaterializeViewMode = DEFAULT,
    currentTime: Long = new Date().getTime) = currentState match {

    case CreatedByViewManager(view) => {
      ResultingViewSchedulingState(
        Waiting(view,
          defaultDigest,
          0,
          view.dependencies.toSet,
          Set(listener),
          materializationMode,
          false,
          false,
          0l),
        view.dependencies.map { Materialize(_, view, materializationMode) }.toSet)
    }

    case ReadFromSchemaManager(view, lastTransformationChecksum, lastTransformationTimestamp) => {
      ResultingViewSchedulingState(
        Waiting(view,
          lastTransformationChecksum,
          lastTransformationTimestamp,
          view.dependencies.toSet,
          Set(listener),
          materializationMode,
          false,
          false,
          0l),
        view.dependencies.map { Materialize(_, view, materializationMode) }.toSet)
    }

    case NoData(view) => {
      ResultingViewSchedulingState(
        Waiting(view,
          view.transformation().checksum,
          0,
          view.dependencies.toSet,
          Set(listener),
          materializationMode,
          false,
          false,
          0l),
        view.dependencies.map { Materialize(_, view, materializationMode) }.toSet)
    }

    case Invalidated(view) => {
      ResultingViewSchedulingState(
        Waiting(view,
          view.transformation().checksum,
          0,
          view.dependencies.toSet,
          Set(listener),
          materializationMode,
          false,
          false,
          0l),
        view.dependencies.map { Materialize(_, view, materializationMode) }.toSet)
    }

    case Waiting(
      view,
      lastTransformationChecksum,
      lastTransformationTimestamp,
      dependenciesMaterializing,
      listenersWaitingForMaterialize,
      materializationMode,
      oneDependencyReturnedData,
      incomplete,
      dependenciesFreshness) => {
      ResultingViewSchedulingState(
        Waiting(
          view,
          lastTransformationChecksum,
          lastTransformationTimestamp,
          dependenciesMaterializing,
          listenersWaitingForMaterialize + listener,
          materializationMode,
          oneDependencyReturnedData,
          incomplete,
          dependenciesFreshness), Set())
    }

    case Materialized(view, lastTransformationChecksum, lastTransformationTimestamp, _, _) => {
      ResultingViewSchedulingState(
        Waiting(view,
          lastTransformationChecksum,
          lastTransformationTimestamp,
          view.dependencies.toSet,
          Set(listener),
          materializationMode,
          false,
          false,
          0l),
        view.dependencies.map { Materialize(_, view, materializationMode) }.toSet)
    }
  }

  def invalidate(
    currentState: ViewSchedulingState,
    issuer: PartyInterestedInViewSchedulingStateChange) =
    ResultingViewSchedulingState(
      Invalidated(currentState.view),
      Set(
        ReportInvalidated(currentState.view, Set(issuer))))

}