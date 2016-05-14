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

import org.scalatest.{ FlatSpec, Matchers }
import org.schedoscope.test.test

class NodeTagsTest extends FlatSpec
    with Matchers {

  "stage.NodeTags" should "load correctly from file" in {
    new NodeTags() with test {
      then()
      numRows shouldBe 10
      row(v(nodeId) shouldBe 122317,
        v(key) shouldBe "TMC:cid_58:tabcd_1:Class",
        v(value) shouldBe "Point")
      row(v(nodeId) shouldBe 122317,
        v(key) shouldBe "TMC:cid_58:tabcd_1:Direction",
        v(value) shouldBe "positive")
      row(v(nodeId) shouldBe 122317,
        v(key) shouldBe "TMC:cid_58:tabcd_1:LCLversion",
        v(value) shouldBe "8.00")
    }
  }
}
