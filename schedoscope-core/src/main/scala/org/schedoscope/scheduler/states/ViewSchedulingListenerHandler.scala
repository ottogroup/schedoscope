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
  * e.g. for gathering statistics and logging information about View status
  *
  */
trait ViewSchedulingListenerHandler {

  /**
    * This method is called only if a View's State is changed
    * For more detailed monitoring, use abstract class
    */
  def viewScheduleStateChange(run: ViewSchedulingListenerHandle):Unit

  /**
    * This method is called after a new View scheduling Action
    * is issued - ONLY if a View State did NOT change
    */
  def viewScheduleNewAction(handle: ViewSchedulingListenerHandle):Unit = {}

}
