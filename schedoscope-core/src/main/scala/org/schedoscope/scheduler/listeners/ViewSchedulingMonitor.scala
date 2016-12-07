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

import org.schedoscope.scheduler.states.{ViewSchedulingEvent, ViewSchedulingState}
import org.slf4j.LoggerFactory

/**
  * Listener aims to monitor view scheduling evolution for these main questions:
  * - How long (approximately) did it take for a view to change state (ViewActor point of view)
  * - If any, how many retries did take place for a given transformation?
  * - What are the detail properties of a view at different scheduling states?
  */
class ViewSchedulingMonitor extends ViewSchedulingListener {

  val log = LoggerFactory.getLogger(getClass)

  override def viewSchedulingEvent(event: ViewSchedulingEvent): Unit = {
    super.viewSchedulingEvent(event)
  }


}
