/**
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
import org.joda.time.LocalDateTime

/**
  * Handle for the view scheduling state and optionally action monitored executed
  * by a view scheduling listener actor. It should allow external parties to monitor
  * evolution of states for views, as well as different actions called on it.
  *
  * Note: A view always has a new state (newState); in case the view is initiated,
  * then previous state (prevState) is set to None
  */
case class ViewSchedulingEvent(view: View,
                               eventTime: LocalDateTime,
                               monitStart: LocalDateTime,
                               action: Option[ViewSchedulingAction],
                               prevState: String,
                               newState: String)
