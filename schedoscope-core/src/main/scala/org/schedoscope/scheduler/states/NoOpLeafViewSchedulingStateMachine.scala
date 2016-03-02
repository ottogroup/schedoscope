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
import org.schedoscope.scheduler.messages.MaterializeViewMode._

/**
 * This class implements a ViewSchedulingStateMachine for views with NoOp transformations.
 */
class NoOpLeafViewSchedulingStateMachine(successFlagExists: => Boolean) extends ViewSchedulingStateMachine {
  
  def materialize(
    currentState: ViewSchedulingState,
    listener: PartyInterestedInViewSchedulingStateChange,
    materializationMode: MaterializeViewMode = DEFAULT,
    currentTime: Long = new Date().getTime) = currentState match {

    case CreatedByViewManager(view) =>
      if (successFlagExists)
        ResultingViewSchedulingState(
          Materialized(
            view,
            view.transformation().checksum,
            currentTime,
            withErrors = false,
            incomplete = false),
          Set(
            WriteTransformationTimestamp(view, currentTime),
            WriteTransformationCheckum(view),
            ReportMaterialized(
              view,
              Set(listener),
              currentTime,
              withErrors = false,
              incomplete = false)))
      else
        ResultingViewSchedulingState(
          NoData(view),
          Set(ReportNoDataAvailable(view, Set(listener))))

    case ReadFromSchemaManager(view, checksum, lastTransformationTimestamp) =>
      ResultingViewSchedulingState(
        Materialized(
          view,
          checksum,
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

    case Invalidated(view) =>
      if (successFlagExists)
        ResultingViewSchedulingState(
          Materialized(
            view,
            view.transformation().checksum,
            currentTime,
            withErrors = false,
            incomplete = false),
          Set(
            WriteTransformationTimestamp(view, currentTime),
            WriteTransformationCheckum(view),
            ReportMaterialized(
              view,
              Set(listener),
              currentTime,
              withErrors = false,
              incomplete = false)))
      else
        ResultingViewSchedulingState(
          NoData(view),
          Set(ReportNoDataAvailable(view, Set(listener))))

    case NoData(view) =>
      if (successFlagExists)
        ResultingViewSchedulingState(
          Materialized(
            view,
            view.transformation().checksum,
            currentTime,
            withErrors = false,
            incomplete = false),
          Set(
            WriteTransformationTimestamp(view, currentTime),
            WriteTransformationCheckum(view),
            ReportMaterialized(
              view,
              Set(listener),
              currentTime,
              withErrors = false,
              incomplete = false)))
      else
        ResultingViewSchedulingState(
          NoData(view),
          Set(ReportNoDataAvailable(view, Set(listener))))

    case Materialized(view, checksum, lastTransformationTimestamp, _, _) =>
      ResultingViewSchedulingState(
        Materialized(
          view,
          checksum,
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
  }

  def invalidate(
    currentState: ViewSchedulingState,
    issuer: PartyInterestedInViewSchedulingStateChange) =
    ResultingViewSchedulingState(
      Invalidated(currentState.view),
      Set(
        ReportInvalidated(currentState.view, Set(issuer))))

  def noDataAvailable(currentState: Waiting, reportingDependency: View, currentTime: Long = new Date().getTime) = ???

  def failed(currentState: Waiting, reportingDependency: View, currentTime: Long = new Date().getTime) = ???

  def materialized(currentState: Waiting, reportingDependency: View, transformationTimestamp: Long, currentTime: Long = new Date().getTime) = ???

  def transformationSucceeded(currentState: Transforming, folderEmpty: => Boolean, currentTime: Long = new Date().getTime) = ???

  def transformationFailed(currentState: Transforming, maxRetries: Int = Schedoscope.settings.retries, currentTime: Long = new Date().getTime) = ???

}