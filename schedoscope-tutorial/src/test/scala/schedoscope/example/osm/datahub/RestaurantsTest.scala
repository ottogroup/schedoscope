package schedoscope.example.osm.datahub

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import schedoscope.example.osm.processed.Nodes
import org.schedoscope.test.rows
import org.schedoscope.dsl.Parameter.p
import org.schedoscope.dsl.Field._
import org.schedoscope.test.test

case class RestaurantsTest() extends FlatSpec
  with Matchers {

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
        v(restaurant_name) shouldBe "Cuore Mio",
        v(restaurant_type) shouldBe "italian",
        v(area) shouldBe "t1y06x1")
    }
  }
}