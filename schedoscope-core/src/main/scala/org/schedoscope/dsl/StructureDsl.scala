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
package org.schedoscope.dsl

trait StructureDsl {
  protected def registerField(f: Field[_]): Unit

  def fieldOf[T: Manifest](orderWeight: Int, nameOverride: String) = {
    val f = Field[T](orderWeight, nameOverride)
    registerField(f)
    f
  }

  def fieldOf[T: Manifest](orderWeight: Int): Field[T] = fieldOf[T](orderWeight, null)

  def fieldOf[T: Manifest](nameOverride: String): Field[T] = fieldOf[T](100, nameOverride)

  def fieldOf[T: Manifest]: Field[T] = fieldOf[T](100, null)
}