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
package org.schedoscope.dsl.views

import java.util.Date

import org.schedoscope.dsl.ViewDsl

/**
 * A standard trait for basic job metadata for views.
 */
trait JobMetadata extends ViewDsl {
  val createdAt = fieldOf[Date](1, "Timestamp of the job run that created the record")
  val createdBy = fieldOf[String](0, "Identifier of the job run creating the record")
}