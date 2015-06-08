package schedoscope.example.osm.stage

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.schedoscope.test.test

class RelationTagsTest extends FlatSpec
  with Matchers {

  "stage.RelationTags" should "load correctly from file" in {
    new RelationTags() with test {
      then()
      numRows shouldBe 26218
      row(v(relation_id) shouldBe "2202",
        v(key) shouldBe "destination",
        v(value) shouldBe "Alster")
      row(v(relation_id) shouldBe "2202",
        v(key) shouldBe "name",
        v(value) shouldBe "Osterbek")
      row(v(relation_id) shouldBe "2202",
        v(key) shouldBe "ref:fgkz",
        v(value) shouldBe "595674")
    }
  }
}
