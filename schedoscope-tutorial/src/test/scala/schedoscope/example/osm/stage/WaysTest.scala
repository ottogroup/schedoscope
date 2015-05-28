package schedoscope.example.osm.stage

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.schedoscope.test.test

class WaysTest extends FlatSpec
    with Matchers {

  "stage.Ways" should "load correctly from file" in {
    new Ways() with test {
      then()
      numRows shouldBe 422235
      row(v(id) shouldBe 1978,
        v(tstamp) shouldBe "2014-03-11 00:34:02+0100",
        v(version) shouldBe 31,
        v(user_id) shouldBe 161619,
        v(changeset_id) shouldBe 21036622)
      row(v(id) shouldBe 1880371,
        v(tstamp) shouldBe "2012-03-04 09:24:37+0100",
        v(version) shouldBe 9,
        v(user_id) shouldBe 63375,
        v(changeset_id) shouldBe 10865588)
      row(v(id) shouldBe 1880372,
        v(tstamp) shouldBe "2014-01-21 09:44:58+0100",
        v(version) shouldBe 6,
        v(user_id) shouldBe 1852,
        v(changeset_id) shouldBe 20118576)
    }
  }
}
