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

import scala.language.{existentials, implicitConversions}

/**
  * A case class for keeping any object with its manifest.
  */
case class TypedAny(v: Any, t: Manifest[_])

/**
  * Implicit function making a typed any out of any object.
  */
object TypedAny {

  /**
    * Convert any object into a typed any.
    */
  implicit def typedAny[V: Manifest](v: V) = TypedAny(v, manifest[V])

}