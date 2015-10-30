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
package org.schedoscope.test

import org.schedoscope.dsl._

/**
 * a testStruct is a Structure that can hold values. Such structs can be tested.
 * 
 */
trait testStruct extends Structure with values {

  /**
   * Set the fields in the teststruct to the values contained in values
   * @param v
   */
  def filledBy(values:Map[String,Any]):Unit= {
   values.foreach{ a=>setByName(a._1,a._2)}
  }
}