package schedoscope.example.osm.datahub

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import schedoscope.example.osm.processed.Nodes
import org.schedoscope.test.rows
import org.schedoscope.dsl.Parameter.p
import org.schedoscope.dsl.Field._
import org.schedoscope.test.test

case class TrainstationsTest() extends FlatSpec
  with Matchers {

  val nodesInput = new Nodes(p("2014"), p("09")) with rows {
    set(v(id, "122317"),
      v(geohash, "t1y140djfcq0"),
      v(tags, Map("name" -> "Hagenbecks Tierpark",
        "railway" -> "station")))
    set(v(id, "274850441"),
      v(geohash, "t1y87ki9fcq0"),
      v(tags, Map("name" -> "Bönningstedt",
        "railway" -> "station")))
    set(v(id, "279023080"),
      v(geohash, "t1y77d8jfcq0"),
      v(tags, Map("name" -> "Harburg",
        "railway" -> "station")))
    set(v(id, "279023080"),
      v(geohash, "t1y77d8jfcq0"),
      v(tags, Map("name" -> "Wachtelstraße")))
  }

  "datahub.Trainstations" should "load correctly from processed.nodes" in {
    new Trainstations() with test {
      basedOn(nodesInput)
      withConfiguration(
          ("INPUT" -> nodesInput.fullPath),
          ("exec.type" -> "LOCAL")
          )      
      then()
      numRows shouldBe 3
      row(v(id) shouldBe "122317",
        v(station_name) shouldBe "Hagenbecks Tierpark",
        v(area) shouldBe "t1y140d")
      row(v(id) shouldBe "274850441",
        v(station_name) shouldBe "Bönningstedt",
        v(area) shouldBe "t1y87ki")
    }
  }
}