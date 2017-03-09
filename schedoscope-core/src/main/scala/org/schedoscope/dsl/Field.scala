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
  * A field-like capturing view fields. Fields have an orderWeight determining their ordering and can override the
  * namingBase inherited from FieldLike.
  */
class Field[T: Manifest](val orderWeight: Long, val nameOverride: Option[String]) extends FieldLike[T] with Commentable {
  override def namingBase = nameOverride.getOrElse(super.namingBase)
}

/**
  * Helpers for fields-
  */
object Field {

  /**
    * Used to assign fields values in the test framework.
    */
  def v[T](f: Field[T], v: T) = (f, v)

  def apply[U: Manifest](orderWeight: Long, nameOverride: Option[String]): Field[U] =
    new Field(orderWeight, nameOverride)
}