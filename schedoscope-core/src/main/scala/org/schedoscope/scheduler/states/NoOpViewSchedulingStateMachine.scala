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

import org.schedoscope.dsl.View
import org.schedoscope.scheduler.messages.MaterializeViewMode._

class NoOpViewSchedulingStateMachine(successFlagExists: => Boolean) extends ViewSchedulingStateMachineImpl {

  override def toWaitingTransformingOrMaterialize(view: View, lastTransformationChecksum: String, lastTransformationTimestamp: Long, listener: PartyInterestedInViewSchedulingStateChange, materializationMode: MaterializeViewMode, currentTime: Long) = {
    if (view.dependencies.isEmpty)
      leaveWaitingState(
        Waiting(view,
          if (lastTransformationTimestamp > 0) lastTransformationChecksum else "RESETME",
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
      super.toWaitingTransformingOrMaterialize(view, lastTransformationChecksum, lastTransformationTimestamp, listener, materializationMode, currentTime)
  }

  override def leaveWaitingState(currentState: Waiting, setIncomplete: Boolean, setError: Boolean, currentTime: Long) = currentState match {
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

      if (oneDependencyReturnedData)
        if (lastTransformationTimestamp < dependenciesFreshness || lastTransformationChecksum != view.transformation().checksum)
          if (successFlagExists)
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
          else
            ResultingViewSchedulingState(
              NoData(view),
              Set(
                ReportNoDataAvailable(view, listenersWaitingForMaterialize)))
        else
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
      else
        ResultingViewSchedulingState(
          NoData(view),
          Set(
            ReportNoDataAvailable(view, listenersWaitingForMaterialize)))

  }
}