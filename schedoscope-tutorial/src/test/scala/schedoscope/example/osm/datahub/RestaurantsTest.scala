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

case class RestaurantsTest() extends SchedoscopeSpec {

  val nodes = new Nodes(p("2014"), p("09")) with rows {
    set(v(id, "267622930"),
      v(geohash, "t1y06x1xfcq0"),
      v(tags, Map("name" -> "Cuore Mio",
        "cuisine" -> "italian",
        "amenity" -> "restaurant")))
    set(v(id, "288858596"),
      v(geohash, "t1y1716cfcq0"),
      v(tags, Map("name" -> "Jam Jam",
        "cuisine" -> "japanese",
        "amenity" -> "restaurant")))
    set(v(id, "302281521"),
      v(geohash, "t1y17m91fcq0"),
      v(tags, Map("name" -> "Walddörfer Croque Café",
        "cuisine" -> "burger",
        "amenity" -> "restaurant")))
    set(v(id, "30228"),
      v(geohash, "t1y77d8jfcq0"),
      v(tags, Map("name" -> "Giovanni",
        "cuisine" -> "italian")))
  }

  "datahub.Restaurants" should "load correctly from processed.nodes" in {
    new Restaurants() with test {
      basedOn(nodes)
      then()
      numRows shouldBe 3
      row(v(id) shouldBe "267622930",
        v(restaurantName) shouldBe "Cuore Mio",
        v(restaurantType) shouldBe "italian",
        v(area) shouldBe "t1y06x1")
    }
  }
}