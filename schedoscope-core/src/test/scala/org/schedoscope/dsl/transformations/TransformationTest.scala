package org.schedoscope.dsl.transformations

import org.scalatest.{FlatSpec, Matchers}

class TransformationTest extends FlatSpec with Matchers {

  "the overwrite checksum mechanism" should "change a checksum" in {
    //new transformation
    val transformation = HiveTransformation("select * from view")

    val checksum1 = transformation.checksum
    //overwrite checksum with the same string as the hiveql
    transformation.overwriteChecksum("select * from view")

    val checksum2 = transformation.checksum

    //change checksum wiht version
    transformation.overwriteChecksum("v2.2")

    val checksum3 = transformation.checksum

    checksum1 shouldBe checksum2

    checksum3 should not be checksum1
    checksum3 should not be checksum2

  }

}
