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
package schedoscope.example.osm.processed

import org.schedoscope.dsl.Field._
import org.schedoscope.test.{SchedoscopeSpec, rows, test}

class NodesWithGeohashTest extends SchedoscopeSpec {

  val stageNodesInput = new schedoscope.example.osm.stage.Nodes() with rows {
    set(
      v(longitude, 10.0232716),
      v(latitude, 53.5282633))
    set(
      v(longitude, 10.0243161),
      v(latitude, 53.5297589))
  }

  "NodesWithGeoHash" should "load correctly from stage.nodes" in {
    new NodesWithGeohash() with test {
      basedOn(stageNodesInput)
      withConfiguration("input_path", stageNodesInput.fullPath)
      then
      numRows shouldBe 2
      row(v(geohash) shouldBe "t1y140djfcq0")
      row(v(geohash) shouldBe "t1y140g5vgcn")
    }

  }
}