package schedoscope.example.osm.stage

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.schedoscope.test.test

class NodesTest extends FlatSpec
  with Matchers {

  "stage.Nodes" should "load correctly from classpath" in {
    new Nodes() with test {
      then()
      numRows shouldBe 2243290
      row(v(id) shouldBe 122317,
        v(tstamp) shouldBe "2014-10-17T13:49:26Z",
        v(version) shouldBe 7,
        v(user_id) shouldBe 50299,
        v(longitude) shouldBe 10.0232716,
        v(latitude) shouldBe 53.5282633)
      row(v(id) shouldBe 122318,
        v(tstamp) shouldBe "2014-10-17 15:49:26Z",
        v(version) shouldBe 6,
        v(user_id) shouldBe 50299,
        v(longitude) shouldBe 10.0243161,
        v(latitude) shouldBe 53.5297589)
      row(v(id) shouldBe 122320,
        v(tstamp) shouldBe "2013-12-20 08:43:33Z",
        v(version) shouldBe 4,
        v(user_id) shouldBe 51991,
        v(longitude) shouldBe 10.02931146,
        v(latitude) shouldBe 53.5351834)
    }
  }
}