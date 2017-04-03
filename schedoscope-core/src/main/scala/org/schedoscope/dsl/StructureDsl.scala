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

/**
  * A trait summarizing the DSL constructs available for the definition of structures and
  * - implicitly - views.
  */
trait StructureDsl extends Named with Commentable {

  protected def registerField(f: Field[_]): Unit

  /**
    * Define a field with default order weight, no comment, and no name override
    */
  def fieldOf[T: Manifest]: Field[T] = fieldOf[T](100, null, null)

  /**
    * Define a field with a specific order weight, no comment, and no name override
    */
  def fieldOf[T: Manifest](orderWeight: Int): Field[T] = fieldOf[T](orderWeight, null, null)

  /**
    * Define a field with default order weight, a comment, and no name override
    */
  def fieldOf[T: Manifest](comment: String): Field[T] = fieldOf[T](100, comment, null)

  /**
    * Define a field with default order weight, a comment, and a name override
    */
  def fieldOf[T: Manifest](comment: String, overrideName: String): Field[T] = fieldOf[T](100, comment, overrideName)

  /**
    * Define a field with a given order weight, comment, and name override.
    */
  def fieldOf[T: Manifest](orderWeight: Int = 100, comment: String = null, overrideName: String = null) = {
    val f = Field[T](orderWeight, if (overrideName != null) Some(overrideName) else None)

    if (comment != null)
      f.comment(comment)

    registerField(f)
    f
  }
}