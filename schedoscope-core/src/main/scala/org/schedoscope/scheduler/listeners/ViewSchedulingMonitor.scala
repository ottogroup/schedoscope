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

import org.schedoscope.scheduler.states.{ViewSchedulingEvent}
import org.slf4j.LoggerFactory

/**
  * Listener aims to monitor view scheduling evolution and
  * their scheduling states details
  *
  */
class ViewSchedulingMonitor extends ViewSchedulingListener {

  val log = LoggerFactory.getLogger(getClass)

  override def viewSchedulingEvent(event: ViewSchedulingEvent): Unit = {
    logStateChange(event)
    logStateDetails(event)
    logViewSchedulingTimeDeltaOutput(event)
    logScheduledActions(event)

    storeNewEvent(event)
  }

  def logStateChange(event: ViewSchedulingEvent) =
    if (event.prevState != event.newState)
      log.info(getMonitInit(event.prevState.view) + getViewStateChangeInfo(event))

  def logStateDetails(event: ViewSchedulingEvent) =
    log.info(getMonitInit(event.prevState.view) + parseAnyState(event.newState))

  def logScheduledActions(event: ViewSchedulingEvent) =
    log.info(getMonitInit(event.prevState.view) +getSetOfActions(event))

  def logViewSchedulingTimeDeltaOutput(event: ViewSchedulingEvent) =
    if(latestViewEvent contains(event.prevState.view))
      log.info(getMonitInit(event.prevState.view) + getViewSchedulingTimeDeltaOutput(event))

}
