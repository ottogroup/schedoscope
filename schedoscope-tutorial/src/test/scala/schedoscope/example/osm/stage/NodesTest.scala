package schedoscope.example.osm.stage

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.schedoscope.test.test

class NodesTest extends FlatSpec
    with Matchers {

  "stage.Nodes" should "load correctly from file" in {
    new Nodes() with test {
      then()
      numRows shouldBe 2240730
      row(v(id) shouldBe 122317,
        v(tstamp) shouldBe "2014-10-17 15:49:26+0200",
        v(version) shouldBe 7,
        v(user_id) shouldBe 50299,
        v(changeset_id) shouldBe 26144995,
        v(postgis_point_column) shouldBe "0101000020E6100000E8D95141EA0B2440AA96BE219EC34A40")
      row(v(id) shouldBe 122318,
        v(tstamp) shouldBe "2014-10-17 15:49:26+0200",
        v(version) shouldBe 6,
        v(user_id) shouldBe 50299,
        v(changeset_id) shouldBe 26144995,
        v(postgis_point_column) shouldBe "0101000020E61000005488EC28730C2440EA21BF23CFC34A40")
      row(v(id) shouldBe 122320,
        v(tstamp) shouldBe "2013-12-20 08:43:33+0100",
        v(version) shouldBe 4,
        v(user_id) shouldBe 51991,
        v(changeset_id) shouldBe 19545194,
        v(postgis_point_column) shouldBe "0101000020E6100000CACC60E7010F2440592EC0E380C44A40")
    }
  }
}