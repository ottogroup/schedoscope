package schedoscope.example.osm.stage

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.schedoscope.test.test

class WayNodesTest extends FlatSpec
    with Matchers {

  "stage.WayNodes" should "load correctly from file" in {
    new WayNodes() with test {
      then()
      numRows shouldBe 2960106
      row(v(way_id) shouldBe 1978,
        v(node_id) shouldBe 10210552,
        v(sequence_id) shouldBe 0)
      row(v(way_id) shouldBe 1978,
        v(node_id) shouldBe 277783046,
        v(sequence_id) shouldBe 1)
      row(v(way_id) shouldBe 1978,
        v(node_id) shouldBe 20834531,
        v(sequence_id) shouldBe 2)
    }
  }
}
