package schedoscope.example.osm.stage

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.schedoscope.test.test

class RelationsTest extends FlatSpec
    with Matchers {

  "stage.Relations" should "load correctly from file" in {
    new Relations() with test {
      then()
      numRows shouldBe 7276
      row(v(id) shouldBe "2202",
        v(occurredAt) shouldBe "2015-04-04 01:51:51+0200",
        v(version) shouldBe 18,
        v(user_id) shouldBe 112934,
        v(changeset_id) shouldBe 29963462)
      row(v(id) shouldBe "2304",
        v(occurredAt) shouldBe "2015-04-24 17:19:36+0200",
        v(version) shouldBe 146,
        v(user_id) shouldBe 795290,
        v(changeset_id) shouldBe 30456239)
      row(v(id) shouldBe "9495",
        v(occurredAt) shouldBe "2009-10-09 08:44:30+0200",
        v(version) shouldBe 22,
        v(user_id) shouldBe 63375,
        v(changeset_id) shouldBe 2789909)
    }
  }
}
