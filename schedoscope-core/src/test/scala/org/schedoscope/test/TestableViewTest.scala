/**
  * Copyright 2015 Otto (GmbH & Co KG)
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package org.schedoscope.test


import org.scalatest.{FlatSpec, Matchers}
import org.schedoscope.dsl.Field.v
import org.schedoscope.dsl.Parameter.p
import org.schedoscope.dsl.storageformats.{Avro, Json, OptimizedRowColumnar, Parquet}
import org.schedoscope.dsl.{ExternalView, View}
import org.schedoscope.dsl.transformations.HiveTransformation.insertInto
import org.schedoscope.dsl.transformations.{HiveTransformation, InvalidTransformationException}
import test.views._

case class View1() extends View {
  val v1 = fieldOf[String]
  val v2 = fieldOf[Integer]

  dependsOn(() => new View2)
}

case class View2() extends View {
  val v1 = fieldOf[Int]
}

case class View3() extends View {
  val v1 = fieldOf[String]
}

case class View4() extends View {
  dependsOn(() => Seq(new View2, new View3))
}

case class View5() extends View {
  val v1 = fieldOf[String]

  val view = dependsOn(() => (1 until 10)
    .map(_ => new View2))

  transformVia(() => HiveTransformation(
    insertInto(this, s"""SELECT * FROM test_org_schedoscope_test.view2"""))
  )
}

case class IllegalJoinOnView() extends View {
  val v1 = fieldOf[String]

  val view = dependsOn(() => (1 until 10)
    .map(_ => new View2))

  dependsOn(() => View3())

  transformVia(() => HiveTransformation(
    insertInto(this, s"""SELECT * FROM test_org_schedoscope_test.view2 JOIN test_org_schedoscope_test.view2"""))
  )
}


class TestableViewTest extends FlatSpec with Matchers {

  val view2i1 = new View2 with rows {
    set(
      v(v1, 2)
    )
  }

  val view2i2 = new View2 with rows {
    set(
      v(v1, 2)
    )
  }

  val view3 = new View3 with rows {
    set(
      v(v1, "3")
    )
  }

  val ec0101Clicks = new Click(p("EC0101"), p("2014"), p("01"), p("01")) with rows {
    set(
      v(id, "event01"),
      v(url, "http://ec0101.com/url1"))
    set(
      v(id, "event02"),
      v(url, "http://ec0101.com/url2"))
    set(
      v(id, "event03"),
      v(url, "http://ec0101.com/url3"))
  }

  val ec0106Clicks = new Click(p("EC0106"), p("2014"), p("01"), p("01")) with rows {
    set(
      v(id, "event04"),
      v(url, "http://ec0106.com/url1"))
    set(
      v(id, "event05"),
      v(url, "http://ec0106.com/url2"))
    set(
      v(id, "event06"),
      v(url, "http://ec0106.com/url3"))
  }

  "the test trait" should "check for valid dependencies" in {
    new View1() with test {
      basedOn(view2i1)
      checkDependencies() shouldBe true
    }
  }


  it should "check invalid dependencies (one valid/ one invalid)" in {
    new View1() with test {
      basedOn(view2i1, view3)
      checkDependencies() shouldBe false
    }
  }

  it should "check invalid dependencies (one invalid)" in {
    new View1() with test {
      basedOn(view3)
      checkDependencies() shouldBe false
    }
  }

  it should "check valid dependencies (empty)" in {
    new View2 with test {
      checkDependencies() shouldBe true
    }
  }

  it should "check valid dependencies (two valid)" in {
    new View4 with test {
      basedOn(view2i1, view3)
      checkDependencies() shouldBe true
    }
  }

  it should "check invalid dependencies (too few) 1" in {
    new View4 with test {
      basedOn(view2i1)
      checkDependencies() shouldBe false
    }
  }

  it should "check invalid dependencies (too few) 2" in {
    new View4 with test {
      basedOn(view3)
      checkDependencies() shouldBe false
    }
  }

  it should "check valid dependencies (duplicate depends on)" in {
    new View4 with test {
      basedOn(view2i1, view2i2, view3)
      checkDependencies() shouldBe true
    }
  }

  it should "check valid dependency loop" in {
    new View5 with test {
      basedOn(view2i1)
      checkDependencies() shouldBe true
    }
  }

  it should "check valid dependencies (both duplicates)" in {
    new View5 with test {
      basedOn(view2i1, view2i2)
      checkDependencies() shouldBe true
    }
  }

  it should "check invalid dependencies (both duplicates one invalid)" in {
    new View5 with test {
      basedOn(view2i1, view2i2, view3)
      checkDependencies() shouldBe false
    }
  }

  it should "throw an exception for invalid dependencies (both duplicates one invalid)" in {
    an[IllegalArgumentException] should be thrownBy {
      new View5 with test {
        basedOn(view2i1, view2i2, view3)
        then()
      }
    }
  }

  it should "not throw an exception for invalid dependencies if check is disabled" in {
    new View5 with test {
      basedOn(view2i1, view2i2, view3)
      then(disableDependencyCheck = true)
    }
  }

  it should "throw an exception for IllegalJoin" in {
    an[InvalidTransformationException] should be thrownBy {
      new IllegalJoinOnView with test {
        basedOn(view2i1, view2i2, view3)
        then(disableLineageValidation = true)
      }
    }
  }

  it should "not throw an exception for IllegalJoin if check is disabled" in {
    new IllegalJoinOnView with test {
      basedOn(view2i1, view2i2, view3)
      then(disableTransformationValidation = true, disableLineageValidation = true)
    }
  }

  it should "forward the test environment to external views" in {
    val productBrand = ProductBrand(p("ec0101"), p("2016"), p("11"), p("07"))
    val externalProductBrand = ExternalView(productBrand)

    externalProductBrand.env = "test"

    productBrand.env shouldBe "test"
    externalProductBrand.env shouldBe "test"
  }

  it should "not change output/goal view storage format Avro" in {
    new ClickEC01Avro(p("2014"), p("01"), p("01")) with test {
      basedOn(ec0101Clicks, ec0106Clicks)
      then()
      storageFormat shouldBe Avro("avro_schemas/click_of_e_c0101_avro.avsc")
      numRows() shouldBe 3
      row(v(id) shouldBe "event01",
        v(url) shouldBe "http://ec0101.com/url1")
      row(v(id) shouldBe "event02",
        v(url) shouldBe "http://ec0101.com/url2")
      row(v(id) shouldBe "event03",
        v(url) shouldBe "http://ec0101.com/url3")
    }
  }

  it should "not change output/goal view storage format ORC" in {
    new ClickEC01ORC(p("2014"), p("01"), p("01")) with test {
      basedOn(ec0101Clicks, ec0106Clicks)
      then()
      storageFormat shouldBe OptimizedRowColumnar()
      numRows() shouldBe 3
      row(v(id) shouldBe "event01",
        v(url) shouldBe "http://ec0101.com/url1")
      row(v(id) shouldBe "event02",
        v(url) shouldBe "http://ec0101.com/url2")
      row(v(id) shouldBe "event03",
        v(url) shouldBe "http://ec0101.com/url3")
    }
  }

  it should "not change output/goal view storage format Parquet" in {
    new ClickEC01Parquet(p("2014"), p("01"), p("01")) with test {
      basedOn(ec0101Clicks, ec0106Clicks)
      then()
      storageFormat shouldBe Parquet()
      numRows() shouldBe 3
      row(v(id) shouldBe "event01",
        v(url) shouldBe "http://ec0101.com/url1")
      row(v(id) shouldBe "event02",
        v(url) shouldBe "http://ec0101.com/url2")
      row(v(id) shouldBe "event03",
        v(url) shouldBe "http://ec0101.com/url3")
    }
  }

  it should "not change output/goal view storage format JSON" in {
    new ClickEC01Json(p("2014"), p("01"), p("01")) with test {
      basedOn(ec0101Clicks, ec0106Clicks)
      then()
      storageFormat shouldBe Json()
      numRows() shouldBe 3
      row(v(id) shouldBe "event01",
        v(url) shouldBe "http://ec0101.com/url1")
      row(v(id) shouldBe "event02",
        v(url) shouldBe "http://ec0101.com/url2")
      row(v(id) shouldBe "event03",
        v(url) shouldBe "http://ec0101.com/url3")
    }
  }

}

