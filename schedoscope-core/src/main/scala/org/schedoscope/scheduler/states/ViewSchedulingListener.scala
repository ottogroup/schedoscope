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

import org.slf4j.LoggerFactory

/**
  * Trait for user defined code to be executed before and after a given View state.
  * e.g. for gathering statistics and logging information about View status
  *
  */
trait ViewSchedulingListener {

  private lazy val log = LoggerFactory.getLogger(getClass)

  def logViewStateChangeInfo(event: ViewSchedulingEvent) =
    if(event.prevState != event.newState)
      log.info(s"VIEW ${event.prevState.view} STATE CHANGE ===> " +
        s"${event.newState.label.toUpperCase()}: newState=${event.newState} " +
        s"previousState=${event.prevState}; actions to perform: [${event.actions.toList.mkString(", ")}]")

  /**
    * This method is called on every incoming View scheduling event
    *
    */
  def viewSchedulingEvent(event: ViewSchedulingEvent):Unit = {
    logViewStateChangeInfo(event)
  }

}
