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

import org.scalatest.{FlatSpec, Matchers}
import org.schedoscope.dsl.Field._
import org.schedoscope.dsl.Parameter.p
import org.schedoscope.test.{rows, test}
import schedoscope.example.osm.processed.Nodes

case class ShopsTest() extends FlatSpec
  with Matchers {

  val nodes = new Nodes(p("2014"), p("09")) with rows {
    set(v(id, "122317"),
      v(geohash, "t1y140djfcq0"),
      v(tags, Map("name" -> "Netto",
        "shop" -> "supermarket")))
    set(v(id, "274850441"),
      v(geohash, "t1y87ki9fcq0"),
      v(tags, Map("name" -> "Schanzenbaeckerei",
        "shop" -> "bakery")))
    set(v(id, "279023080"),
      v(geohash, "t1y77d8jfcq0"),
      v(tags, Map("name" -> "Edeka Linow",
        "shop" -> "supermarket")))
    set(v(id, "279023080"),
      v(geohash, "t1y77d8jfcq0"),
      v(tags, Map("name" -> "Edeka Linow")))
  }

  "datahub.Shops" should "load correctly from processed.nodes" in {
    new Shops() with test {
      basedOn(nodes)
      then()
      numRows shouldBe 3
      row(v(id) shouldBe "122317",
        v(shopName) shouldBe "Netto",
        v(shopType) shouldBe "supermarket",
        v(area) shouldBe "t1y140d")
      row(v(id) shouldBe "274850441",
        v(shopName) shouldBe "Schanzenbaeckerei",
        v(shopType) shouldBe "bakery",
        v(area) shouldBe "t1y87ki")
    }
  }
}
