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

import org.scalatest.{ FlatSpec, Matchers }
import org.schedoscope.dsl.Field._
import org.schedoscope.dsl.Parameter.p
import org.schedoscope.test.{ rows, test }
import schedoscope.example.osm.stage.NodeTags

case class NodesTest() extends FlatSpec
    with Matchers {

  val nodes = new NodesWithGeohash() with rows {
    set(v(id, 122317L),
      v(tstamp, "2014-09-17T13:49:26Z"),
      v(version, 7),
      v(userId, 50299),
      v(longitude, 10.0232716),
      v(latitude, 53.5282633),
      v(geohash, "t1y140djfcq0"))
    set(v(id, 122318L),
      v(tstamp, "2013-06-17 15:49:26Z"),
      v(version, 6),
      v(userId, 50299),
      v(longitude, 10.0243161),
      v(latitude, 53.5297589),
      v(geohash, "t1y140g5vgcn"))
  }

  val nodeTags = new NodeTags() with rows {
    set(v(nodeId, 122317L),
      v(key, "TMC:cid_58:tabcd_1:Class"),
      v(value, "Point"))
    set(v(nodeId, 122318L),
      v(key, "TMC:cid_58:tabcd_1:Direction"),
      v(value, "positive"))
    set(v(nodeId, 122318L),
      v(key, "TMC:cid_58:tabcd_1:LCLversion"),
      v(value, "8.00"))
    set(v(nodeId, 122318L),
      v(key, "TMC:cid_58:tabcd_1:LocationCode"),
      v(value, "10696"))
  }

  "processed.Nodes" should "load correctly from processed.nodes_with_geohash and stage.node_tags" in {
    new Nodes(p("2013"), p("06")) with test {
      basedOn(nodeTags, nodes)
      then()
      numRows shouldBe 1
      row(v(id) shouldBe "122318",
        v(occurredAt) shouldBe "2013-06-17 15:49:26Z",
        v(version) shouldBe 6,
        v(userId) shouldBe 50299,
        v(tags) shouldBe Map(
          "TMC:cid_58:tabcd_1:Direction" -> "positive",
          "TMC:cid_58:tabcd_1:LCLversion" -> "8.00",
          "TMC:cid_58:tabcd_1:LocationCode" -> "10696"))
    }
  }
}