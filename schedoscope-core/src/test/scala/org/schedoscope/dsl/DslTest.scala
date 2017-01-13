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
package org.schedoscope.dsl

import java.util.Date

import org.scalatest.{FlatSpec, Matchers}
import org.schedoscope.dsl.Parameter.p
import org.schedoscope.dsl.TypedAny.typedAny
import org.schedoscope.dsl.storageformats.{Avro, Parquet, TextFile}
import org.schedoscope.dsl.transformations.{HiveTransformation, NoOp}
import org.schedoscope.dsl.views.{DailyParameterization, JobMetadata, PointOccurrence}
import org.schedoscope.schema.ddl.HiveQl._
import test.extviews.ExternalShop
import test.views._

class DslTest extends FlatSpec with Matchers {

  "A view" should "be instantiable without new" in {
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrandView should not be null
  }

  it should "have a name which is based on its class name and partitioning suffixes" in {
    val brandView = Brand(p("ec0106"))

    brandView.n shouldEqual "brand_ec0106"
  }

  it should "have a module name which is defined by its package" in {
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrandView.module shouldEqual "test_views"
  }

  it should "have a job name which is defined by its package and class" in {
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrandView.jobName shouldEqual "d_t_views.product_brand/ec0106/2014/01/01/20140101"
  }

  it should "be commentable" in {
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrandView.comment.get shouldEqual "ProductBrand joins brands with products"
  }

  it should "be able to read the name of its fields" in {
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrandView.brandName.n shouldEqual "brand_name"
  }

  it should "be able to read the name of its Parameters" in {
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrandView.shopCode.n shouldEqual "shop_code"
  }

  it should "be able to assign values to parameters" in {
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrandView.shopCode.v shouldEqual (Some("ec0106"))
  }

  it should "be able to read values from parameters explicitly" in {
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    val ecShopCodeRead = productBrandView.shopCode.v.get

    ecShopCodeRead shouldBe "ec0106"

  }

  it should "be able to accesss dependencies" in {
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    val dependency = productBrandView.dependencies(0)

    dependency shouldBe a[View]
  }

  it should "be able to traverse to dependencies implicitly" in {
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    val dependency: View = productBrandView.dependencies(0)

    dependency shouldBe a[View]
  }

  it should "collect all dependencies" in {
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrandView.dependencies should contain(productBrandView.product())
    productBrandView.dependencies should contain(productBrandView.brand())

  }

  it should "have a rank according to their level in the dependency hierarchy" in {
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))
    val brandView = productBrandView.brand()
    val productView = productBrandView.product()

    productView.rank shouldBe 0
    brandView.rank shouldBe 0
    productBrandView.rank shouldBe 1
  }

  it should "collect all fields" in {
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))
    val fields = productBrandView.fields.map { f => (f.n, f.t) }

    fields should contain allOf(("product_id", manifest[String]), ("brand_name", manifest[String]), ("occurred_at", manifest[String]), ("created_at", manifest[Date]), ("created_by", manifest[String]))
  }

  it should "collect all parameters" in {
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))
    val parameterTypes = productBrandView.parameters.map { f => (f.n, f.t) }
    val parameterValues = productBrandView.parameters.map { f => (f.n, f.v) }
    parameterTypes should contain allOf(("shop_code", manifest[String]), ("year", manifest[String]), ("month", manifest[String]), ("day", manifest[String]))
    parameterValues should contain allOf(("shop_code", Some("ec0106")), ("year", Some("2014")), ("month", Some("01")), ("day", Some("01")))
  }

  "A view's field types" should "be accessible at both compile and runtime" in {
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrandView.occurredAt.t shouldBe manifest[String]
  }

  "A view's parameter types" should "be accessible at both compile and runtime" in {
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrandView.shopCode.t shouldBe manifest[String]
  }

  "A view's dependency types" should "be accessible at both compile and runtime" in {
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrandView.brand() shouldBe a[Brand]
  }

  "A view's dependencies" should "be settable via dependsOn" in {
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrandView.dependencies.isEmpty shouldBe false
  }

  "A view's transformation" should "default to NoOp" in {
    val product = Brand(p("ec0106"))

    product.transformation() shouldEqual NoOp()
  }

  it should "be settable via transformVia" in {
    val productBrand = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrand.transformation().isInstanceOf[HiveTransformation] shouldBe true
  }

  "A view's parameters" should "partition a view" in {
    val productBrand = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrand.isPartition(productBrand.shopCode) shouldBe true
    productBrand.isPartition(productBrand.year) shouldBe true
    productBrand.isPartition(productBrand.month) shouldBe true
    productBrand.isPartition(productBrand.day) shouldBe true
  }

  "asSuffix" should "partition a view but as a suffix" in {
    val product = Product(p("ec0106"), p("2014"), p("01"), p("01"))

    product.isSuffixPartition(product.shopCode) shouldBe true
    product.isSuffixPartition(product.year) shouldBe false
    product.isSuffixPartition(product.month) shouldBe false
    product.isSuffixPartition(product.day) shouldBe false
  }

  "A view's storage format" should "be settable via storedAs" in {
    val productBrand = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrand.storageFormat shouldEqual Parquet()
  }

  it should "default to TEXTFILE" in {
    val productBrand = Brand(p("ec0106"))

    productBrand.storageFormat shouldEqual TextFile()
  }

  "A view's fields" should "come in the right order" in {
    val productBrand = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrand.fields.map {
      _.n
    } should contain inOrder("occurred_at", "product_id", "brand_name", "created_at", "created_by")
    productBrand.brand().fields.map {
      _.n
    } should contain inOrder("id", "brand_name", "created_at", "created_by")
    productBrand.product().fields.map {
      _.n
    } should contain inOrder("id", "occurred_at", "name", "brand_id", "created_at", "created_by")
  }

  it should "be able to have comments" in {
    val productBrand = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrand.brand().name.comment shouldBe Some("The brand's name, but field name overriden")
  }

  it should "be able to override weights" in {
    val productBrand = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrand.brand().name.orderWeight shouldBe 100
  }

  it should "be able to override names" in {
    val productBrand = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrand.brand().shopCode.n shouldBe "shop_code"
    productBrand.brand().name.n shouldBe "brand_name"
  }

  "A view" should "be transformable into DDL" in {
    val productBrand = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))
    val avro = AvroView(p("2014"), p("01"), p("01"))
    val edgeCases = EdgeCasesView()

    val ddlStatement = ddl(productBrand)

    ddlStatement.contains("CREATE EXTERNAL TABLE IF NOT EXISTS dev_test_views.product_brand (") shouldBe true
    ddlStatement.contains("occurred_at STRING,") shouldBe true
    ddlStatement.contains("product_id STRING,") shouldBe true
    ddlStatement.contains("brand_name STRING,") shouldBe true
    ddlStatement.contains("created_at STRING,") shouldBe true
    ddlStatement.contains("created_by STRING") shouldBe true
    ddlStatement.contains(")") shouldBe true
    ddlStatement.contains("COMMENT 'ProductBrand joins brands with products'") shouldBe true
    ddlStatement.contains("PARTITIONED BY (shop_code STRING, year STRING, month STRING, day STRING, date_id STRING)") shouldBe true
    ddlStatement.contains("STORED AS PARQUET") shouldBe true
    ddlStatement.contains("LOCATION '/hdp/dev/test/views/product_brand'") shouldBe true
  }

  it should "be transformable into DDL for an env" in {
    val productBrand = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))
    productBrand.env = "prod"

    val ddlStatement = ddl(productBrand)

    ddlStatement.contains("CREATE EXTERNAL TABLE IF NOT EXISTS prod_test_views.product_brand (") shouldBe true
    ddlStatement.contains("occurred_at STRING,") shouldBe true
    ddlStatement.contains("product_id STRING,") shouldBe true
    ddlStatement.contains("brand_name STRING,") shouldBe true
    ddlStatement.contains("created_at STRING,") shouldBe true
    ddlStatement.contains("created_by STRING") shouldBe true
    ddlStatement.contains(")") shouldBe true
    ddlStatement.contains("COMMENT 'ProductBrand joins brands with products'") shouldBe true
    ddlStatement.contains("PARTITIONED BY (shop_code STRING, year STRING, month STRING, day STRING, date_id STRING)") shouldBe true
    ddlStatement.contains("STORED AS PARQUET") shouldBe true
    ddlStatement.contains("LOCATION '/hdp/prod/test/views/product_brand'") shouldBe true

  }

  it should "inherit its env to its dependencies" in {
    val productBrand = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))
    productBrand.env = "prod"

    for (d <- productBrand.dependencies) {
      d.env shouldEqual productBrand.env
    }
  }

  it should "have a DDL checksum that varies" in {
    val productBrand = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))
    val avro = AvroView(p("2014"), p("01"), p("01"))

    ddlChecksum(avro) should not equal ddlChecksum(productBrand)
  }

  it should "have a DDL checksum that is resilient to Avro schema changes" in {
    val avro = AvroView(p("2014"), p("01"), p("01"))

    val checksumOriginal = ddlChecksum(avro)

    avro.storageFormat = Avro("new schema")

    val checksumModifiedSchema = ddlChecksum(avro)

    checksumOriginal shouldEqual checksumModifiedSchema
  }


  "A parameter" should "be equal to another one of the same parameterization" in {
    val p1: Parameter[Int] = p(2)
    val p2: Parameter[Int] = p(2)

    (p1 eq p2) shouldBe false
    p1 shouldEqual p2
  }

  it should "not be equal to a parameter with different parameterization" in {
    val p1: Parameter[Int] = p(2)
    val p2: Parameter[Int] = p(3)
    val p3: Parameter[String] = p(2.toString)

    (p1 eq p2) shouldBe false
    (p1 == p2) shouldBe false
    (p1 == p3) shouldBe false
  }

  it should "be constructable from another parameter but have different weight" in {
    val p1 = p(1)
    val pp1 = p(p1)

    p1 shouldBe pp1
    pp1.orderWeight should be > p1.orderWeight
    pp1.v.get shouldBe an[Integer]
  }

  "Views" should "be equal based on their parameters" in {
    val productBrandView1 = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))
    val productBrandView2 = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrandView1 shouldEqual productBrandView2
  }

  it should "share common dependencies" in {
    val productBrandView1 = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))
    val productBrandView2 = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    (productBrandView1 eq productBrandView2) shouldBe false

    (productBrandView1.brand() eq productBrandView2.brand()) shouldBe true
    (productBrandView1.product() eq productBrandView2.product()) shouldBe true
  }

  it should "be dynamically instantiatable" in {
    val productBrandView = View.newView(classOf[ProductBrand], "dev", p("ec0106"), p("2014"), p("01"), p("01"))

    productBrandView shouldEqual ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))
  }

  it should "be dynamically instantiatable via URL path" in {
    val views = View.viewsFromUrl("dev", "/test.views/Product/e(EC0106,EC0101)/rymd(20140224-20131202)/")

    views.length shouldBe 2 * 85

    views.foreach {
      v =>
        val product = v.asInstanceOf[Product]
        val dateString = s"${product.year.v.get}${product.month.v.get}${product.day.v.get}"

        product.shopCode.v.get should (equal("EC0106") or equal("EC0101"))
        dateString should be <= "20140224"
        dateString should be >= "20131202"
    }
  }

  it should "throw an exception during dynamic instantiation" in {
    val thrown = the[java.lang.RuntimeException] thrownBy View.viewsFromUrl("dev", "/test.views/RequireView/ec0106/")
    thrown.getMessage() shouldBe "Error while parsing view(s) /test.views/RequireView/ec0106/ : requirement failed: Put in upper case: ec0106"
  }

  it should "have the same urlPath as the one they were dynamically constructed with" in {
    val views = View.viewsFromUrl("dev", "test.views/Product/EC0106/2014/02/24/20140224")

    views.length shouldBe 1

    val product = views.head.asInstanceOf[Product]

    val productParameters = product.partitionParameters

    product.urlPath shouldBe "test.views/Product/EC0106/2014/02/24/20140224"
  }

  it should "be queryable" in {
    val views = View.viewsInPackage("test.views")

    views should contain allOf(classOf[Brand], classOf[Product], classOf[ProductBrand], classOf[EdgeCasesView], classOf[AvroView], classOf[ViewWithDefaultParams], classOf[Click], classOf[ClickOfEC0101])

    val traits = View.getTraits(classOf[ProductBrand])

    traits should contain theSameElementsAs List(classOf[DailyParameterization], classOf[PointOccurrence], classOf[JobMetadata])
  }

  "A View" should "be marked as external" in {
    val productBrand = ExternalView(ProductBrand(p("ec0106"), p("2014"), p("01"), p("01")))

    productBrand.registeredTransformation() shouldBe NoOp()
    productBrand.dependencies shouldBe empty
    productBrand.transformation() shouldBe NoOp()
    productBrand.registeredExports shouldBe empty
    productBrand.isExternal shouldBe true
  }

  it should "have an external dependency" in {
    val viewWithExternalDep = ViewWithExternalDeps(p("ec0106"), p("2014"), p("01"), p("01"))
    val extView = ExternalShop()

    val dependency = viewWithExternalDep.dependencies.head

    dependency.registeredTransformation() shouldBe NoOp()
    dependency.dependencies shouldBe empty
    dependency.transformation() shouldBe NoOp()
    dependency.registeredExports shouldBe empty
    dependency.isExternal shouldBe true
    dependency.dbPath shouldBe extView.dbPath
    dependency.dbName shouldBe extView.dbName
    dependency.tableName shouldBe extView.tableName
    dependency.parameters shouldBe extView.parameters
    dependency.partitionSpec shouldBe extView.partitionSpec
    dependency.module shouldBe extView.module
    dependency.partitionParameters shouldBe extView.partitionParameters
    dependency.fields shouldBe extView.fields
    dependency.storageFormat shouldBe extView.storageFormat

  }
}
