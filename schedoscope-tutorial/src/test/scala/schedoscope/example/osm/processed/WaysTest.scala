package schedoscope.example.osm.processed

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import org.schedoscope.test.test
import org.schedoscope.test.rows
import org.schedoscope.dsl.Field._
import schedoscope.example.osm.stage.WayNodes
import schedoscope.example.osm.stage.WayTags
import org.schedoscope.dsl.Parameter.p

case class WaysTest() extends FlatSpec
    with Matchers {

  val ways = new schedoscope.example.osm.stage.Ways() with rows {
    set(v(id, 1978L),
      v(tstamp, "2014-03-11 00:34:02+0100"),
      v(version, 31),
      v(user_id, 161619),
      v(changeset_id, 21036622L))
    set(v(id, 1880371L),
      v(tstamp, "2012-03-04 09:24:37+0100"),
      v(version, 9),
      v(user_id, 63375),
      v(changeset_id, 10865588L))
    set(v(id, 1880372L),
      v(tstamp, "2014-11-21 09:44:58+0100"),
      v(version, 6),
      v(user_id, 1852),
      v(changeset_id, 20118576L))
  }

  val wayNodes = new WayNodes() with rows {
    set(v(way_id, 1978L),
      v(node_id, 10210552L),
      v(sequence_id, 0))
    set(v(way_id, 1978L),
      v(node_id, 10210L),
      v(sequence_id, 1))
    set(v(way_id, 1880372L),
      v(node_id, 8810552L),
      v(sequence_id, 0))
    set(v(way_id, 1880372L),
      v(node_id, 88105L),
      v(sequence_id, 1))
    set(v(way_id, 1978L),
      v(node_id, 10210553L),
      v(sequence_id, 2))
    set(v(way_id, 1880371L),
      v(node_id, 5510552L),
      v(sequence_id, 0))
  }

  val wayTags = new WayTags() with rows {
    set(v(way_id, 1978L),
      v(key, "surface"),
      v(value, "asphalt"))
    set(v(way_id, 1978L),
      v(key, "highway"),
      v(value, "residential"))
    set(v(way_id, 1880371L),
      v(key, "maxspeed"),
      v(value, "30"))
    set(v(way_id, 1880372L),
      v(key, "postal_code"),
      v(value, "22085"))
    set(v(way_id, 1978L),
      v(key, "name"),
      v(value, "Averhoffstraße"))
  }

  "processed.Ways" should "load correctly from stage.ways, stage.way_tags and stage.way_nodes" in {
    new Ways(p("2014"), p("11")) with test {
      basedOn(ways, wayNodes, wayTags)
      then()
      numRows shouldBe 1
      row(v(id) shouldBe "1880372",
        v(occurredAt) shouldBe "2014-11-21 09:44:58+0100",
        v(version) shouldBe 6,
        v(user_id) shouldBe 1852,
        v(nodes) shouldBe List("8810552", "88105"),
        v(tags) shouldBe Map("postal_code" -> "22085"))
    }
  }
}