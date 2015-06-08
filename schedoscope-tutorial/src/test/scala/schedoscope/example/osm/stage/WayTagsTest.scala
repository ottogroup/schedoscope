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
