package org.schedoscope.scheduler.states

import java.util.Date
import org.schedoscope.scheduler.messages.MaterializeViewMode._
import org.schedoscope.dsl.transformations.Checksum.defaultDigest
import org.schedoscope.dsl.View
import org.schedoscope.Schedoscope

class IntermediateViewSchedulingStateMachine extends ViewSchedulingStateMachine {

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
          false,
          0l),
        view.dependencies.map { Materialize(_, view, materializationMode) }.toSet)
    }

    case NoData(view) => {
      ResultingViewSchedulingState(
        Waiting(view,
          defaultDigest,
          0,
          view.dependencies.toSet,
          Set(listener),
          materializationMode,
          false,
          false,
          false,
          0l),
        view.dependencies.map { Materialize(_, view, materializationMode) }.toSet)
    }

    case Invalidated(view) => {
      ResultingViewSchedulingState(
        Waiting(view,
          defaultDigest,
          0,
          view.dependencies.toSet,
          Set(listener),
          materializationMode,
          false,
          false,
          false,
          0l),
        view.dependencies.map { Materialize(_, view, materializationMode) }.toSet)
    }

    case Failed(view) => {
      ResultingViewSchedulingState(
        Waiting(view,
          defaultDigest,
          0,
          view.dependencies.toSet,
          Set(listener),
          materializationMode,
          false,
          false,
          false,
          0l),
        view.dependencies.map { Materialize(_, view, materializationMode) }.toSet)
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
      withErrors,
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
          withErrors,
          dependenciesFreshness), Set())
    }

    case Retrying(
      view,
      lastTransformationChecksum,
      materializationMode,
      listenersWaitingForMaterialize,
      withErrors,
      incomplete,
      retry) => {
      ResultingViewSchedulingState(
        Transforming(
          view,
          lastTransformationChecksum,
          listenersWaitingForMaterialize + listener,
          materializationMode,
          withErrors,
          incomplete,
          retry), Set())
    }

    case Transforming(
      view,
      lastTransformationChecksum,
      listenersWaitingForMaterialize,
      materializationMode,
      withErrors,
      incomplete,
      retry) => {
      ResultingViewSchedulingState(
        Transforming(
          view,
          lastTransformationChecksum,
          listenersWaitingForMaterialize + listener,
          materializationMode,
          withErrors,
          incomplete,
          retry), Set())
    }
  }

  def invalidate(
    currentState: ViewSchedulingState,
    issuer: PartyInterestedInViewSchedulingStateChange) = currentState match {

    case waiting: Waiting => ResultingViewSchedulingState(
      waiting, Set(ReportNotInvalidated(waiting.view, Set(issuer))))

    case transforming: Transforming => ResultingViewSchedulingState(
      transforming, Set(ReportNotInvalidated(transforming.view, Set(issuer))))

    case retrying: Retrying => ResultingViewSchedulingState(
      retrying, Set(ReportNotInvalidated(retrying.view, Set(issuer))))

    case _ => ResultingViewSchedulingState(
      Invalidated(currentState.view),
      Set(
        ReportInvalidated(currentState.view, Set(issuer))))
  }

  private def leaveWaitingState(currentState: Waiting, setIncomplete: Boolean, setError: Boolean, currentTime: Long) = currentState match {
    case Waiting(
      view,
      lastTransformationChecksum,
      lastTransformationTimestamp,
      dependenciesMaterializing,
      listenersWaitingForMaterialize,
      materializationMode,
      oneDependencyReturnedData,
      incomplete,
      withErrors,
      dependenciesFreshness) => {

      if (oneDependencyReturnedData) {
        if (lastTransformationTimestamp < dependenciesFreshness || lastTransformationChecksum != view.transformation().checksum) {
          if (materializationMode == RESET_TRANSFORMATION_CHECKSUMS_AND_TIMESTAMPS)
            ResultingViewSchedulingState(
              Materialized(
                view,
                view.transformation().checksum,
                currentTime,
                incomplete | setIncomplete,
                withErrors | setError),
              {
                if (lastTransformationChecksum != view.transformation().checksum)
                  Set(WriteTransformationCheckum(view))
                else
                  Set()
              } ++
                Set(
                  WriteTransformationTimestamp(view, currentTime),
                  ReportMaterialized(
                    view,
                    listenersWaitingForMaterialize,
                    currentTime,
                    incomplete | setIncomplete,
                    withErrors | setError)))
          else
            ResultingViewSchedulingState(
              Transforming(
                view,
                lastTransformationChecksum,
                listenersWaitingForMaterialize,
                materializationMode,
                withErrors,
                incomplete,
                0),
              Set())
        } else
          ResultingViewSchedulingState(
            Materialized(
              view,
              lastTransformationChecksum,
              lastTransformationTimestamp,
              incomplete | setIncomplete,
              withErrors | setError),
            Set(
              ReportMaterialized(
                view,
                listenersWaitingForMaterialize,
                lastTransformationTimestamp,
                incomplete | setIncomplete,
                withErrors | setError))
              ++ {
                if (materializationMode == RESET_TRANSFORMATION_CHECKSUMS)
                  Set(WriteTransformationCheckum(view))
                else
                  Set()
              })
      } else
        ResultingViewSchedulingState(
          NoData(view),
          Set(
            ReportNoDataAvailable(view, listenersWaitingForMaterialize)))
    }
  }

  def noDataAvailable(currentState: Waiting, reportingDependency: View, successFlagExists: => Boolean, currentTime: Long = new Date().getTime) = currentState match {
    case Waiting(
      view,
      lastTransformationChecksum,
      lastTransformationTimestamp,
      dependenciesMaterializing,
      listenersWaitingForMaterialize,
      materializationMode,
      oneDependencyReturnedData,
      incomplete,
      withErrors,
      dependenciesFreshness) => {
      if (dependenciesMaterializing == Set(reportingDependency))
        leaveWaitingState(currentState, true, false, currentTime)
      else
        ResultingViewSchedulingState(
          Waiting(
            view,
            lastTransformationChecksum,
            lastTransformationTimestamp,
            dependenciesMaterializing - reportingDependency,
            listenersWaitingForMaterialize,
            materializationMode,
            oneDependencyReturnedData,
            true,
            withErrors,
            dependenciesFreshness), Set())
    }
  }

  def failed(currentState: Waiting, reportingDependency: View, successFlagExists: => Boolean, currentTime: Long = new Date().getTime) = currentState match {
    case Waiting(
      view,
      lastTransformationChecksum,
      lastTransformationTimestamp,
      dependenciesMaterializing,
      listenersWaitingForMaterialize,
      materializationMode,
      oneDependencyReturnedData,
      incomplete,
      withErrors,
      dependenciesFreshness) => {
      if (dependenciesMaterializing == Set(reportingDependency))
        leaveWaitingState(currentState, true, true, currentTime)
      else
        ResultingViewSchedulingState(
          Waiting(
            view,
            lastTransformationChecksum,
            lastTransformationTimestamp,
            dependenciesMaterializing - reportingDependency,
            listenersWaitingForMaterialize,
            materializationMode,
            oneDependencyReturnedData,
            true,
            true,
            dependenciesFreshness), Set())
    }
  }

  def materialized(currentState: Waiting, reportingDependency: View, transformationTimestamp: Long, successFlagExists: => Boolean, currentTime: Long = new Date().getTime) = currentState match {
    case Waiting(
      view,
      lastTransformationChecksum,
      lastTransformationTimestamp,
      dependenciesMaterializing,
      listenersWaitingForMaterialize,
      materializationMode,
      oneDependencyReturnedData,
      incomplete,
      withErrors,
      dependenciesFreshness) => {

      val updatedWaitingState = Waiting(
        view,
        lastTransformationChecksum,
        lastTransformationTimestamp,
        dependenciesMaterializing - reportingDependency,
        listenersWaitingForMaterialize,
        materializationMode,
        true,
        incomplete,
        withErrors,
        Math.max(dependenciesFreshness, transformationTimestamp))

      if (dependenciesMaterializing == Set(reportingDependency))
        leaveWaitingState(updatedWaitingState, false, false, currentTime)
      else
        ResultingViewSchedulingState(updatedWaitingState, Set())
    }
  }

  def transformationSucceeded(currentState: Transforming, currentTime: Long = new Date().getTime) = currentState match {
    case Transforming(
      view,
      lastTransformationChecksum,
      listenersWaitingForMaterialize,
      materializationMode,
      withErrors,
      incomplete,
      retry) => {
      ResultingViewSchedulingState(
        Materialized(
          view,
          view.transformation().checksum,
          currentTime,
          incomplete,
          withErrors),
        {
          if (materializationMode == RESET_TRANSFORMATION_CHECKSUMS || lastTransformationChecksum != view.transformation().checksum)
            Set(WriteTransformationCheckum(view))
          else
            Set()
        } ++
          Set(
            WriteTransformationTimestamp(view, currentTime),
            ReportMaterialized(
              view,
              listenersWaitingForMaterialize,
              currentTime,
              incomplete,
              withErrors)))
    }
  }

  def transformationFailed(currentState: Transforming, maxRetries: Int = Schedoscope.settings.retries, currentTime: Long = new Date().getTime) = currentState match {
    case Transforming(
      view,
      lastTransformationChecksum,
      listenersWaitingForMaterialize,
      materializationMode,
      withErrors,
      incomplete,
      retry) => {
      if (retry < maxRetries)
        ResultingViewSchedulingState(
          Retrying(
            view,
            lastTransformationChecksum,
            materializationMode,
            listenersWaitingForMaterialize,
            withErrors,
            incomplete,
            retry + 1),
          Set())
      else
        ResultingViewSchedulingState(
          Failed(view),
          Set(ReportFailed(view, listenersWaitingForMaterialize)))
    }
  }
}