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

import org.schedoscope.dsl.ViewDsl

/**
 * A trait defining a standard ID field, with maximum weigth so that the ID is the first field of the view.
 */
trait Id extends ViewDsl {
  val id = fieldOf[String](Int.MaxValue, "The ID of the entity represented by the view")
}