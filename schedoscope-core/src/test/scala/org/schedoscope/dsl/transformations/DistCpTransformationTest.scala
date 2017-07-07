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
package org.schedoscope.dsl.transformations

import _root_.test.views.ProductBrand
import org.apache.hadoop.fs.Path
import org.schedoscope.Schedoscope
import org.schedoscope.dsl.storageformats.{Parquet, TextFile}
import org.schedoscope.dsl.views.{DailyParameterization, JobMetadata, PointOccurrence}
import org.schedoscope.dsl.{Parameter, View}
import org.schedoscope.test.{SchedoscopeSpec, rows, test}


case class CopyProductBrand(shopCode: Parameter[String],
                            year: Parameter[String],
                            month: Parameter[String],
                            day: Parameter[String]) extends View
  with PointOccurrence
  with JobMetadata
  with DailyParameterization {

  comment("ProductBrand joins brands with products")

  val productId = fieldOf[String]
  val brandName = fieldOf[String]

  val product = dependsOn(() => ProductBrand(shopCode, year, month, day))



  transformVia(() => DistCpTransformation.copyToView(product(), this))
  storedAs(TextFile(fieldTerminator = "\t"))
}


class DistCpTransformationTest extends SchedoscopeSpec {

  import org.schedoscope.dsl.Field._
  import org.schedoscope.dsl.Parameter._
  import test.views._

  val productBrandView = new ProductBrand(p("ec0106"), p("2014"), p("01"), p("01")) with rows {
    set(
      v(productId, "id"),
      v(brandName, "brand")
    )
  }

  "DistCp" should "copy a file" in {
    new CopyProductBrand(p("ec0106"), p("2014"), p("01"), p("01")) with test {
      basedOn(productBrandView)
      val conf = DistCpConfiguration()
      conf.sourcePaths = List(new Path(productBrandView.fullPath))
      withConfiguration(conf)
      then()
      numRows() shouldBe 1
      row(
        v(productId) shouldBe "id",
        v(brandName) shouldBe "brand")
    }
  }

}
