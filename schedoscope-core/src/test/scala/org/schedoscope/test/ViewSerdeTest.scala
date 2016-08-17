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

import org.scalatest.{FlatSpec, Matchers}
import org.schedoscope.DriverTests
import org.schedoscope.dsl.Structure

case class Contingency() extends Structure {
  val o11 = fieldOf[Long]
  val o12 = fieldOf[Long]
  val o21 = fieldOf[Long]
  val o22 = fieldOf[Long]
}

case class Nested() extends Structure {
  val value = fieldOf[Long]
  val nested = fieldOf[Contingency]
}

class ViewSerdeTest extends FlatSpec with Matchers {

  "view Serde" should "deserialize structures" taggedAs (DriverTests) in {
    val c = new Contingency()
    val json = """{"value":1, "nested":{"o11":1,"o12":0,"o21":0,"o22":11}}"""
    val res = ViewSerDe.deserializeField(manifest[Nested], json)
    assert(res == Map("value" -> 1, "nested" -> Map("o11" -> 1, "o12" -> 0, "o21" -> 0, "o22" -> 11)))

  }
  "view Serde" should "deserialize Lists" taggedAs (DriverTests) in {
    assert(ViewSerDe.deserializeField(manifest[List[Long]], "[]") == List())
    assert(ViewSerDe.deserializeField(manifest[List[Long]], "[1,2,3]") == List(1, 2, 3))

  }
}