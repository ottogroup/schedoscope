package org.schedoscope.dsl.transformations

import org.scalatest.{FlatSpec, Matchers}
import org.schedoscope.dsl.View
import org.schedoscope.dsl.transformations.HiveTransformation._

case class HiveView() extends View {

  val f = fieldOf[String]

  transformVia {
    () => HiveTransformation(
      insertInto(this, "select * from view"))
        .defineVersion("v2.2")
  }

}

class TransformationTest extends FlatSpec with Matchers {

  "the define version method" should "change a checksum" in {
    //new transformation
    val transformation = HiveTransformation("select * from view")

    val checksum1 = transformation.checksum
    //overwrite checksum with the same string as the hiveql
    transformation.defineVersion("select * from view")

    val checksum2 = transformation.checksum

    //change checksum with version
    transformation.defineVersion("v2.2")

    val checksum3 = transformation.checksum

    checksum1 shouldBe Checksum.digest("select * from view")

    checksum1 shouldBe checksum2

    checksum3 should not be checksum1
    checksum3 should not be checksum2

  }

  it should "change the checksum of a transformation in a view" in {
    //new transformation
   val view = new HiveView()

    view.transformation().checksum shouldBe Checksum.digest("v2.2")

  }
}
