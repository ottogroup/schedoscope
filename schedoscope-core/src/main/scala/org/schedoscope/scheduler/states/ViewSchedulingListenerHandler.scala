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

/**
  * Trait for user defined code to be executed before and after a given View state.
  * e.g. for gathering statistics
  * and logging information about precise status of Views
  *
  */
trait ViewSchedulingListenerHandler {

  /**
    * This method is called immediately after a View scheduling action was started.
    * This can be used to take measurements before the execution of View scheduling
    * action run or other setup tasks.
    */
  def viewScheduleActionStart(run: ViewSchedulingListenerHandle)

  /**
    * This method is called after scheduled action has changed
    *
    */
  def viewScheduleActionCompleted(stateOfCompletion: ViewSchedulingState,
                                  run: ViewSchedulingListenerHandle)

}

/**
  * Default implementation of a View Scheduling Listener handler. Does nothing.
  */
class DoNothingViewSchedulingListenerHandler extends ViewSchedulingListenerHandler {

  def viewScheduleActionStart(run: ViewSchedulingListenerHandle) {}

  def viewScheduleActionCompleted(stateOfCompletion: ViewSchedulingState,
                         run: ViewSchedulingListenerHandle) {}

}