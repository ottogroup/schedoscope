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

class ViewSchedulingListener(viewSchedulingHandlerClassName:String) {

  lazy val viewSchedulingRunCompletionHandler: ViewSchedulingListenerHandler =
    Class
      .forName(viewSchedulingHandlerClassName)
      .newInstance()
      .asInstanceOf[ViewSchedulingListenerHandler]

  /**
    * Call handler state/action related methods
    * Note: avoids duplication => only if action did not
    *       change state does it call viewScheduleNewAction
    *       method
    */
  def viewSchedulingCall(handle: ViewSchedulingListenerHandle): Unit = {
    if (handle.prevState != handle.newState) {
      viewSchedulingRunCompletionHandler.viewScheduleStateChange(handle)
    }
    else {
      viewSchedulingRunCompletionHandler.viewScheduleNewAction(handle)
    }
  }

}
