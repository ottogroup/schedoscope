/**
 * Copyright 2015 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schedoscope.scheduler.states

import java.util.Date

import org.schedoscope.Schedoscope
import org.schedoscope.dsl.View
import org.schedoscope.dsl.transformations.Checksum.defaultDigest
import org.schedoscope.scheduler.messages.MaterializeViewMode._

class BaseViewSchedulingStateMachine extends ViewSchedulingStateMachine {

  def toWaitingTransformingOrMaterialize(view: View, lastTransformationChecksum: String, lastTransformationTimestamp: Long, listener: PartyInterestedInViewSchedulingStateChange, materializationMode: MaterializeViewMode, currentTime: Long) = {
    if (view.isMaterializeOnce && lastTransformationTimestamp > 0)
      ResultingViewSchedulingState(
        Materialized(
          view,
          lastTransformationChecksum,
          lastTransformationTimestamp,
          withErrors = false,
          incomplete = false),
        Set(
          ReportMaterialized(
            view,
            Set(listener),
            lastTransformationTimestamp,
            withErrors = false,
            incomplete = false))
          ++ {
            if (materializationMode == RESET_TRANSFORMATION_CHECKSUMS)
              Set(WriteTransformationCheckum(view))
            else
              Set()
          })
    else if (view.dependencies.isEmpty)
      fromWaitingToTransformingOrMaterialize(
        Waiting(view,
          lastTransformationChecksum,
          lastTransformationTimestamp,
          Set(),
          Set(listener),
          materializationMode,
          oneDependencyReturnedData = true,
          withErrors = false,
          incomplete = false,
          0l),
        setIncomplete = false,
        setError = false,
        currentTime)
    else
      ResultingViewSchedulingState(
        Waiting(view,
          lastTransformationChecksum,
          lastTransformationTimestamp,
          view.dependencies.toSet,
          Set(listener),
          materializationMode,
          oneDependencyReturnedData = false,
          withErrors = false,
          incomplete = false,
          0l),
        view.dependencies.map {
          Materialize(_, view, materializationMode)
        }.toSet)
  }

  def fromWaitingToTransformingOrMaterialize(currentState: Waiting, setIncomplete: Boolean, setError: Boolean, currentTime: Long) = currentState match {
    case Waiting(
      view,
      lastTransformationChecksum,
      lastTransformationTimestamp,
      dependenciesMaterializing,
      listenersWaitingForMaterialize,
      materializationMode,
      oneDependencyReturnedData,
      withErrors,
      incomplete,
      dependenciesFreshness) =>

      if (oneDependencyReturnedData) {
        if (lastTransformationTimestamp <= dependenciesFreshness || lastTransformationChecksum != view.transformation().checksum) {
          if (materializationMode == RESET_TRANSFORMATION_CHECKSUMS_AND_TIMESTAMPS)
            ResultingViewSchedulingState(
              Materialized(
                view,
                view.transformation().checksum,
                currentTime,
                withErrors | setError,
                incomplete | setIncomplete), {
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
                    withErrors | setError,
                    incomplete | setIncomplete)))
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
              Set(Transform(view)))
        } else
          ResultingViewSchedulingState(
            Materialized(
              view,
              lastTransformationChecksum,
              lastTransformationTimestamp,
              withErrors | setError,
              incomplete | setIncomplete),
            Set(
              ReportMaterialized(
                view,
                listenersWaitingForMaterialize,
                lastTransformationTimestamp,
                withErrors | setError,
                incomplete | setIncomplete))
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

  def materialize(
    currentState: ViewSchedulingState,
    listener: PartyInterestedInViewSchedulingStateChange,
    successFlagExists: => Boolean,
    materializationMode: MaterializeViewMode = DEFAULT,
    currentTime: Long = new Date().getTime) = currentState match {

    case CreatedByViewManager(view) =>
      toWaitingTransformingOrMaterialize(view, defaultDigest, 0, listener, materializationMode, currentTime)

    case ReadFromSchemaManager(view, lastTransformationChecksum, lastTransformationTimestamp) =>
      toWaitingTransformingOrMaterialize(view, lastTransformationChecksum, lastTransformationTimestamp, listener, materializationMode, currentTime)

    case NoData(view) =>
      toWaitingTransformingOrMaterialize(view, defaultDigest, 0, listener, materializationMode, currentTime)

    case Invalidated(view) =>
      toWaitingTransformingOrMaterialize(view, defaultDigest, 0, listener, materializationMode, currentTime)

    case Materialized(view, lastTransformationChecksum, lastTransformationTimestamp, _, _) =>
      toWaitingTransformingOrMaterialize(view, lastTransformationChecksum, lastTransformationTimestamp, listener, materializationMode, currentTime)

    case Failed(view) =>
      ResultingViewSchedulingState(
        Failed(view),
        Set(ReportFailed(view, Set(listener))))

    case Waiting(
      view,
      lastTransformationChecksum,
      lastTransformationTimestamp,
      dependenciesMaterializing,
      listenersWaitingForMaterialize,
      currentMaterializationMode,
      oneDependencyReturnedData,
      withErrors,
      incomplete,
      dependenciesFreshness) =>
      ResultingViewSchedulingState(
        Waiting(
          view,
          lastTransformationChecksum,
          lastTransformationTimestamp,
          dependenciesMaterializing,
          listenersWaitingForMaterialize + listener,
          currentMaterializationMode,
          oneDependencyReturnedData,
          withErrors,
          incomplete,
          dependenciesFreshness), Set())

    case Retrying(
      view,
      lastTransformationChecksum,
      listenersWaitingForMaterialize,
      currentMaterializationMode,
      withErrors,
      incomplete,
      retry) =>
      ResultingViewSchedulingState(
        Transforming(
          view,
          lastTransformationChecksum,
          listenersWaitingForMaterialize + listener,
          currentMaterializationMode,
          withErrors,
          incomplete,
          retry), Set())

    case Transforming(
      view,
      lastTransformationChecksum,
      listenersWaitingForMaterialize,
      currentMaterializationMode,
      withErrors,
      incomplete,
      retry) =>
      ResultingViewSchedulingState(
        Transforming(
          view,
          lastTransformationChecksum,
          listenersWaitingForMaterialize + listener,
          currentMaterializationMode,
          withErrors,
          incomplete,
          retry), Set())
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

  def noDataAvailable(currentState: Waiting, reportingDependency: View, successFlagExists: => Boolean, currentTime: Long = new Date().getTime) = currentState match {
    case Waiting(
      view,
      lastTransformationChecksum,
      lastTransformationTimestamp,
      dependenciesMaterializing,
      listenersWaitingForMaterialize,
      materializationMode,
      oneDependencyReturnedData,
      withErrors,
      incomplete,
      dependenciesFreshness) =>
      if (dependenciesMaterializing == Set(reportingDependency))
        fromWaitingToTransformingOrMaterialize(currentState, setIncomplete = true, setError = false, currentTime)
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
            withErrors,
            incomplete = true,
            dependenciesFreshness), Set())
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
      withErrors,
      incomplete,
      dependenciesFreshness) =>
      if (dependenciesMaterializing == Set(reportingDependency))
        fromWaitingToTransformingOrMaterialize(currentState, setIncomplete = true, setError = true, currentTime)
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
            withErrors = true,
            incomplete = true,
            dependenciesFreshness), Set())
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
      withErrors,
      incomplete,
      dependenciesFreshness) =>

      val updatedWaitingState = Waiting(
        view,
        lastTransformationChecksum,
        lastTransformationTimestamp,
        dependenciesMaterializing - reportingDependency,
        listenersWaitingForMaterialize,
        materializationMode,
        oneDependencyReturnedData = true,
        withErrors = incomplete,
        incomplete = withErrors,
        Math.max(dependenciesFreshness, transformationTimestamp))

      if (dependenciesMaterializing == Set(reportingDependency))
        fromWaitingToTransformingOrMaterialize(updatedWaitingState, setIncomplete = false, setError = false, currentTime)
      else
        ResultingViewSchedulingState(updatedWaitingState, Set())
  }

  def transformationSucceeded(currentState: Transforming, currentTime: Long = new Date().getTime) = currentState match {
    case Transforming(
      view,
      lastTransformationChecksum,
      listenersWaitingForMaterialize,
      materializationMode,
      withErrors,
      incomplete,
      retry) =>
      ResultingViewSchedulingState(
        Materialized(
          view,
          view.transformation().checksum,
          currentTime,
          incomplete,
          withErrors), {
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
              withErrors,
              incomplete)))
  }

  def transformationFailed(currentState: Transforming, maxRetries: Int = Schedoscope.settings.retries, currentTime: Long = new Date().getTime) = currentState match {
    case Transforming(
      view,
      lastTransformationChecksum,
      listenersWaitingForMaterialize,
      materializationMode,
      withErrors,
      incomplete,
      retry) =>
      if (retry < maxRetries)
        ResultingViewSchedulingState(
          Retrying(
            view,
            lastTransformationChecksum,
            listenersWaitingForMaterialize,
            materializationMode,
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