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
package org.schedoscope.scheduler.listeners

import org.schedoscope.scheduler.states.ViewSchedulingEvent

class ViewSchedulingListenerHandler(viewSchedulingHandlerClassName: String) {

  lazy val viewSchedulingListener: ViewSchedulingListener =
    Class
      .forName(viewSchedulingHandlerClassName)
      .newInstance()
      .asInstanceOf[ViewSchedulingListener]

  /**
    * Call handler passing event data
    */
  def viewSchedulingCall(event: ViewSchedulingEvent): Unit = {
    viewSchedulingListener.viewSchedulingEvent(event)
  }

}

/**
  * Explicit exception in order to cause ViewSchedulingListener Actor's restart
  * with latest view scheduling event per View recovery
  */
case class RetryableViewSchedulingListenerException(message: String = null, cause: Throwable = null) extends RuntimeException(message, cause)

/**
  * General purpose exception, which forces ViewSchedulingListener Actor to restart
  * without view scheduling events recovery
  */
case class ViewSchedulingListenerException(message: String = null, cause: Throwable = null) extends RuntimeException(message, cause)
