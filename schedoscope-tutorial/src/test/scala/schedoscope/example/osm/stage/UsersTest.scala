package schedoscope.example.osm.stage

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.schedoscope.test.test

class UsersTest extends FlatSpec
    with Matchers {

  "stage.Users" should "load correctly from file" in {
    new Users() with test {
      then()
      numRows shouldBe 2673
      row(v(id) shouldBe "50299",
        v(name) shouldBe "Todeskuh")
      row(v(id) shouldBe "51991",
        v(name) shouldBe "Osmonav")
      row(v(id) shouldBe "349191",
        v(name) shouldBe "glühwürmchen")
    }
  }
}
