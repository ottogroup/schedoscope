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
import shapeless.TypeOperators.T

trait ViewSchedulingListener {

  /**
    * Needs to be overridden to return the class names of View Action run completion handlers to apply.
    *
    * E.g., provide a val of the same name to the constructor of the ViewSchedulingListener implementation.
    */
  def viewSchedulingRunCompletionHandlerClassName: String

  lazy val viewSchedulingRunCompletionHandler: ViewSchedulingListenerHandler =
    Class
      .forName(viewSchedulingRunCompletionHandlerClassName)
      .newInstance()
      .asInstanceOf[ViewSchedulingListenerHandler]


  /**
    * The views actively being monitored by this Monitor at a given point.
    */
  var viewsActivelyMonitored: Set[View]

  /**
    * Get the current view run state for a given view represented by the handle.
    */
  def getViewSchedulingState(run: ViewSchedulingListenerHandle): ViewSchedulingState

  /**
    * Confirm if there has been a change in the current scheduled action for a given
    * view run state for a given view represented by the handle.
    */
  def viewSchedulingActionChange(newRun: ViewSchedulingListenerHandle,
                                prevRun: ViewSchedulingListenerHandle): Boolean

  /**
    * Create a view state change listener upon view initialization of a given View,
    */
  def init(view: View): ViewSchedulingListenerHandle

  /**
    * Actually starts the ViewScheduling Action listener upon an initialized View
    */
  def viewSchedulingRunStarted(newRun: ViewSchedulingListenerHandle,
                               prevRun: ViewSchedulingListenerHandle) = {
    if(viewSchedulingActionChange(newRun, prevRun)) {
      viewSchedulingRunCompleted(newRun: ViewSchedulingListenerHandle)
    }
    viewSchedulingRunCompletionHandler.viewScheduleActionStart(newRun)
  }



  /**
    * Invokes completion handlers after the given driver run.
    */
  def viewSchedulingRunCompleted(run: ViewSchedulingListenerHandle) {
    getViewSchedulingState(run) match {
      case vss: CreatedByViewManager => viewSchedulingRunCompletionHandler.viewScheduleActionCompleted(vss, run)
      case vss: Failed => viewSchedulingRunCompletionHandler.viewScheduleActionCompleted(vss, run)
      case vss: Invalidated => viewSchedulingRunCompletionHandler.viewScheduleActionCompleted(vss, run)
      case vss: Materialized => viewSchedulingRunCompletionHandler.viewScheduleActionCompleted(vss, run)
      case vss: NoData => viewSchedulingRunCompletionHandler.viewScheduleActionCompleted(vss, run)
      case vss: ReadFromSchemaManager => viewSchedulingRunCompletionHandler.viewScheduleActionCompleted(vss, run)
      case vss: Retrying => viewSchedulingRunCompletionHandler.viewScheduleActionCompleted(vss, run)
      case vss: Transforming => viewSchedulingRunCompletionHandler.viewScheduleActionCompleted(vss, run)
      case vss: ViewSchedulingState => viewSchedulingRunCompletionHandler.viewScheduleActionCompleted(vss, run)
      case vss: Waiting => viewSchedulingRunCompletionHandler.viewScheduleActionCompleted(vss, run)
      case _ => throw new RuntimeException("View Schedule state returned is not valid.")
    }
  }


}
