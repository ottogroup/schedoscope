package schedoscope.example.osm.stage

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.schedoscope.test.test

class NodeTagsTest extends FlatSpec
    with Matchers {

  "stage.NodeTags" should "load correctly from file" in {
    new NodeTags() with test {
      then()
      numRows shouldBe 411748
      row(v(node_id) shouldBe "122317",
        v(key) shouldBe "TMC:cid_58:tabcd_1:Class",
        v(value) shouldBe "Point")
      row(v(node_id) shouldBe "122317",
        v(key) shouldBe "TMC:cid_58:tabcd_1:Direction",
        v(value) shouldBe "positive")
      row(v(node_id) shouldBe "122317",
        v(key) shouldBe "TMC:cid_58:tabcd_1:LCLversion",
        v(value) shouldBe "8.00")
    }
  }
}
