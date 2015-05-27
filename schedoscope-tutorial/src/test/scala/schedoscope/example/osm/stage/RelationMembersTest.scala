package schedoscope.example.osm.stage

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.schedoscope.test.test

class RelationMembersTest extends FlatSpec
    with Matchers {

  "stage.RelationMembers" should "load correctly from file" in {
    new RelationMembers() with test {
      then()
      numRows shouldBe 173870
      row(v(relation_id) shouldBe "2202",
        v(member_id) shouldBe "54040789",
        v(member_type) shouldBe "W",
        v(member_role) shouldBe "side_stream",
        v(sequence_id) shouldBe 0)
      row(v(relation_id) shouldBe "2202",
        v(member_id) shouldBe "53903997",
        v(member_type) shouldBe "W",
        v(member_role) shouldBe "side_stream",
        v(sequence_id) shouldBe 1)
        row(v(relation_id) shouldBe "2202",
        v(member_id) shouldBe "320370233",
        v(member_type) shouldBe "W",
        v(member_role) shouldBe "side_stream",
        v(sequence_id) shouldBe 2) 
    }
  }
}
