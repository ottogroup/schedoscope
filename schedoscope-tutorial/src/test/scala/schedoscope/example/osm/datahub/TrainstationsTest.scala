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
package schedoscope.example.osm.datahub

import org.schedoscope.dsl.Field._
import org.schedoscope.dsl.Parameter.p
import org.schedoscope.test.{SchedoscopeSpec, rows, test}
import schedoscope.example.osm.processed.Nodes

class TrainstationsTest extends SchedoscopeSpec {

  val nodesInput = new Nodes(p("2014"), p("09")) with rows {
    set(v(id, "122317"),
      v(geohash, "t1y140djfcq0"),
      v(tags, Map("name" -> "Hagenbecks Tierpark",
        "railway" -> "station")))
    set(v(id, "274850441"),
      v(geohash, "t1y87ki9fcq0"),
      v(tags, Map("name" -> "Boenningstedt",
        "railway" -> "station")))
    set(v(id, "279023080"),
      v(geohash, "t1y77d8jfcq0"),
      v(tags, Map("name" -> "Harburg",
        "railway" -> "station")))
    set(v(id, "279023080"),
      v(geohash, "t1y77d8jfcq0"),
      v(tags, Map("name" -> "Wachtelstrasse")))
  }

  "datahub.Trainstations" should "load correctly from processed.nodes" in {
    new Trainstations() with test {
      basedOn(nodesInput)
      withConfiguration(
        ("exec.type" -> "LOCAL"))
      then()
      numRows shouldBe 3
      row(v(id) shouldBe "122317",
        v(stationName) shouldBe "Hagenbecks Tierpark",
        v(area) shouldBe "t1y140d")
      row(v(id) shouldBe "274850441",
        v(stationName) shouldBe "Boenningstedt",
        v(area) shouldBe "t1y87ki")
    }
  }
}
