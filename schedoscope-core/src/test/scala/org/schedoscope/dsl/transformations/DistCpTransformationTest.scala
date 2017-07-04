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
    println(Schedoscope.settings.viewDataHdfsRoot)
    new CopyProductBrand(p("ec0106"), p("2014"), p("01"), p("01")) with test {
      basedOn(productBrandView)
      val conf = DistCpConfiguration()
      conf.sourcePaths = List(new Path(productBrandView.fullPath))
      withConfiguration(conf)
      then()
      numRows() shouldBe 1
      println(rowData)
      row(
        v(productId) shouldBe "id",
        v(brandName) shouldBe "brand")
    }
  }

}
