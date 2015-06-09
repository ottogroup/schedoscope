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
package schedoscope.example.osm.stage

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.schedoscope.test.test

class WayTagsTest extends FlatSpec
  with Matchers {

  "stage.WayTags" should "load correctly from file" in {
    new WayTags() with test {
      then()
      numRows shouldBe 1499650
      row(v(way_id) shouldBe 1978,
        v(key) shouldBe "cycleway",
        v(value) shouldBe "track")
      row(v(way_id) shouldBe 1978,
        v(key) shouldBe "highway",
        v(value) shouldBe "primary")
      row(v(way_id) shouldBe 1978,
        v(key) shouldBe "maxspeed",
        v(value) shouldBe "50")
    }
  }
}
