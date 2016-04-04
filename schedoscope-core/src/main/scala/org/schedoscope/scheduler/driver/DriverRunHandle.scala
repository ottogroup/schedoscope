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
package org.schedoscope.scheduler.driver

import org.schedoscope.dsl.transformations.Transformation
import org.joda.time.LocalDateTime

/**
 * Handle for the transformation executed by a driver, called a driver run.
 *
 * The real, technology-specific handle for a executions of a transformation type is kept
 * in the property stateHandle.
 */
class DriverRunHandle[T <: Transformation](val driver: Driver[T], val started: LocalDateTime, val transformation: T, val stateHandle: Any)

