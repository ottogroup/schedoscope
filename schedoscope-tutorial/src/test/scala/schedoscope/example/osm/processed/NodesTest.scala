package schedoscope.example.osm.processed

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import org.schedoscope.test.test
import org.schedoscope.test.rows
import org.schedoscope.dsl.Field._
import schedoscope.example.osm.stage.NodeTags

case class NodesTest() extends FlatSpec
    with Matchers {

  val nodes = new NodesWithGeohash() with rows {
    set(v(id, 122317L),
      v(tstamp, "2014-09-17T13:49:26Z"),
      v(version, 7),
      v(user_id, 50299),
      v(longitude, 10.0232716),
      v(latitude, 53.5282633),
      v(geohash, "t1y140djfcq0"))
    set(v(id, 122318L),
      v(tstamp, "2014-10-17 15:49:26Z"),
      v(version, 6),
      v(user_id, 50299),
      v(longitude, 10.0243161),
      v(latitude, 53.5297589),
      v(geohash, "t1y140g5vgcn"))
  }

  val nodeTags = new NodeTags() with rows {
    set(v(node_id, 122317L),
      v(key, "TMC:cid_58:tabcd_1:Class"),
      v(value, "Point"))
    set(v(node_id, 122318L),
      v(key, "TMC:cid_58:tabcd_1:Direction"),
      v(value, "positive"))
    set(v(node_id, 122318L),
      v(key, "TMC:cid_58:tabcd_1:LCLversion"),
      v(value, "8.00"))
    set(v(node_id, 122318L),
      v(key, "TMC:cid_58:tabcd_1:LocationCode"),
      v(value, "10696"))
  }
  
  "processed.Nodes" should "load correctly from processed.nodes_with_geohash and stage.node_tags" in {
    new Nodes() with test {
      basedOn(nodeTags, nodes)
      then()
      numRows shouldBe 1
      //      row(v(id) shouldBe "1880372",
      //        v(occurredAt) shouldBe "2014-10-21 09:44:58+0100",
      //        v(version) shouldBe 6,
      //        v(user_id) shouldBe 1852
      //        ,
      //        v(nodes) shouldBe Map(),
      //        v(tags) shouldBe 0
      //        )
      //      row(v(id) shouldBe "1880372",
      //        v(occurredAt) shouldBe "2014-10-21 09:44:58+0100",
      //        v(version) shouldBe 6,
      //        v(user_id) shouldBe 1852,
      //        v(changeset_id) shouldBe 20118576,
      //        v(node_id) shouldBe "88105",
      //        v(sequence_id) shouldBe 1)
      //      row(v(id) shouldBe 1978,
      //        v(occurredAt) shouldBe "2014-03-11 00:34:02+0100",
      //        v(version) shouldBe 31,
      //        v(user_id) shouldBe 161619,
      //        v(changeset_id) shouldBe 21036622)

      //      row(v(id) shouldBe 1880371,
      //        v(tstamp) shouldBe "2012-03-04 09:24:37+0100",
      //        v(version) shouldBe 9,
      //        v(user_id) shouldBe 63375,
      //        v(changeset_id) shouldBe 10865588)
    }
  }
}