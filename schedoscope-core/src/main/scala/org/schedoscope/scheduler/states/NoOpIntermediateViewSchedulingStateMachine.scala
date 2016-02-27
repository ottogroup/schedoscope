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

class NoOpIntermediateViewSchedulingStateMachine extends ViewSchedulingStateMachine {

  def materialize(
    currentState: ViewSchedulingState,
    listener: PartyInterestedInViewSchedulingStateChange,
    successFlagExists: => Boolean,
    materializationMode: MaterializeViewMode = DEFAULT,
    currentTime: Long = new Date().getTime) = currentState match {

    case CreatedByViewManager(view) =>
      ResultingViewSchedulingState(
        Waiting(view,
          defaultDigest,
          0,
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

    case ReadFromSchemaManager(view, lastTransformationChecksum, lastTransformationTimestamp) =>
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

    case NoData(view) =>
      ResultingViewSchedulingState(
        Waiting(view,
          defaultDigest,
          0,
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

    case Invalidated(view) =>
      ResultingViewSchedulingState(
        Waiting(view,
          defaultDigest,
          0,
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

    case Waiting(
      view,
      lastTransformationChecksum,
      lastTransformationTimestamp,
      dependenciesMaterializing,
      listenersWaitingForMaterialize,
      `materializationMode`,
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
          materializationMode,
          oneDependencyReturnedData,
          withErrors,
          incomplete,
          dependenciesFreshness), Set())

    case Materialized(view, lastTransformationChecksum, lastTransformationTimestamp, _, _) =>
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

  def invalidate(
    currentState: ViewSchedulingState,
    issuer: PartyInterestedInViewSchedulingStateChange) = currentState match {

    case waiting: Waiting => ResultingViewSchedulingState(
      waiting, Set(ReportNotInvalidated(waiting.view, Set(issuer))))

    case _ => ResultingViewSchedulingState(
      Invalidated(currentState.view),
      Set(
        ReportInvalidated(currentState.view, Set(issuer))))
  }

  private def leaveWaitingState(currentState: Waiting, setIncomplete: Boolean, setError: Boolean, successFlagExists: => Boolean, currentTime: Long) = currentState match {
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

      if (oneDependencyReturnedData && successFlagExists) {
        if (lastTransformationTimestamp < dependenciesFreshness || lastTransformationChecksum != view.transformation().checksum) {
          ResultingViewSchedulingState(
            Materialized(
              view,
              view.transformation().checksum,
              currentTime,
              withErrors | setError,
              incomplete | setIncomplete), {
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
                  withErrors | setError,
                  incomplete | setIncomplete)))
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
        leaveWaitingState(currentState, setIncomplete = true, setError = false, successFlagExists = successFlagExists, currentTime)
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
        leaveWaitingState(currentState, setIncomplete = true, setError = true, successFlagExists = successFlagExists, currentTime)
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
        withErrors = withErrors,
        incomplete = incomplete,

        Math.max(dependenciesFreshness, transformationTimestamp))

      if (dependenciesMaterializing == Set(reportingDependency))
        leaveWaitingState(updatedWaitingState, setIncomplete = false, setError = false, successFlagExists = successFlagExists, currentTime)
      else
        ResultingViewSchedulingState(updatedWaitingState, Set())
  }

  def transformationSucceeded(currentState: Transforming, currentTime: Long = new Date().getTime) = ???

  def transformationFailed(currentState: Transforming, maxRetries: Int = Schedoscope.settings.retries, currentTime: Long = new Date().getTime) = ???

}