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
package org.schedoscope.test

import org.schedoscope.scheduler.states.{ViewSchedulingListenerHandle, ViewSchedulingListenerHandler, ViewSchedulingListenerHandlerInternalException}


class TestViewListenerHandler extends ViewSchedulingListenerHandler {

  override def viewScheduleStateChange(run: ViewSchedulingListenerHandle):Unit = {
    throw new ViewSchedulingListenerHandlerInternalException("Cool, it works well!")
  }


  override def viewScheduleNewAction(handle: ViewSchedulingListenerHandle):Unit = {
    throw new ViewSchedulingListenerHandlerInternalException("And the second too, we're on a lucky streak!")
  }
}

