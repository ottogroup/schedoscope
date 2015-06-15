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
package schedoscope.example.osm.datamart

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.schedoscope.test.rows
import org.schedoscope.dsl.Field._
import org.schedoscope.test.test
import schedoscope.example.osm.datahub.Trainstations
import schedoscope.example.osm.datahub.Restaurants
import schedoscope.example.osm.datahub.Shops

case class ShopProfilesTest() extends FlatSpec
  with Matchers {

  val shops = new Shops() with rows {
    set(v(id, "122546"),
      v(shop_name, "Netto"),
      v(shop_type, "supermarket"),
      v(area, "t1y87ki"))
    set(v(id, "274850441"),
      v(shop_name, "Schanzenbäckerei"),
      v(shop_type, "bakery"),
      v(area, "t1y87ki"))
    set(v(id, "279023080"),
      v(shop_name, "Edeka Linow"),
      v(shop_type, "supermarket"),
      v(area, "t1y77d8"))
  }

  val restaurants = new Restaurants() with rows {
    set(v(id, "267622930"),
      v(restaurant_name, "Cuore Mio"),
      v(restaurant_type, "italian"),
      v(area, "t1y06x1"))
    set(v(id, "288858596"),
      v(restaurant_name, "Jam Jam"),
      v(restaurant_type, "japanese"),
      v(area, "t1y87ki"))
    set(v(id, "302281521"),
      v(restaurant_name, "Walddörfer Croque Café"),
      v(restaurant_type, "burger"),
      v(area, "t1y17m9"))
  }

  val trainstations = new Trainstations() with rows {
    set(v(id, "122317"),
      v(station_name, "Hagenbecks Tierpark"),
      v(area, "t1y140d"))
    set(v(id, "122317"),
      v(station_name, "Bönningstedt"),
      v(area, "t1y87ki"))
  }

  "datamart.ShopProfiles" should "load correctly from datahub.shops, datahub.restaurants, datahub.trainstations" in {
    new ShopProfiles() with test {
      basedOn(shops, restaurants, trainstations)
      then()
      numRows shouldBe 3
      row(v(id) shouldBe "122546",
        v(shop_name) shouldBe "Netto",
        v(shop_type) shouldBe "supermarket",
        v(area) shouldBe "t1y87ki",
        v(cnt_competitors) shouldBe 1,
        v(cnt_restaurants) shouldBe 1,
        v(cnt_trainstations) shouldBe 1)
    }
  }
}