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
  * A trait for entities that have a name, e.g., views, fields, and parameters.
  */
trait Named {

  /**
    * The base name of the Named entity.
    */
  def namingBase: String

  /**
    * The base name of the Named entity converted to a database-friendly format, i.e., using only lowercase / underscore.
    */
  def n = Named.camelToLowerUnderscore(namingBase)
}

/**
  * Helpers for Named entities
  */
object Named {

  /**
    * converts camel case to lower_case / underscore format.
    */
  def camelToLowerUnderscore(name: String) = {
    val formattedName = new StringBuffer()
    for (c <- name) {
      if (c >= 'A' && c <= 'Z')
        formattedName.append("_" + c.toString.toLowerCase())
      else
        formattedName.append(c)
    }
    formattedName.toString().replaceAll("^_", "")
  }
}