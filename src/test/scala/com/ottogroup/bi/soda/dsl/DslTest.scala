package com.ottogroup.bi.soda.dsl

import java.util.Date
import org.scalatest.FlatSpec
import org.scalatest.Matchers
iimport org.schedoscope.dsl.Parameter;
import org.schedoscope.dsl.View;
import org.schedoscope.dsl.Parameter.p;
import org.schedoscope.dsl.View.t;
mport com.ottogroup.bi.soda.crate.ddl.HiveQl.ddl
import com.ottogroup.bi.soda.dsl.transformations.HiveTransformation
import com.ottogroup.bi.soda.dsl.views.DailyParameterization
import com.ottogroup.bi.soda.dsl.views.JobMetadata
import com.ottogroup.bi.soda.dsl.views.PointOccurrence
import test.eci.datahub.AvroView
import test.eci.datahub.Brand
import test.eci.datahub.Click
import test.eci.datahub.ClickOfEC0101
import test.eci.datahub.ClickOfEC0101ViaOozie
import test.eci.datahub.EdgeCasesView
import test.eci.datahub.Product
import test.eci.datahub.ProductBrand
import test.eci.datahub.ViewWithDefaultParams
import org.schedoscope.dsl.Parameter

class DslTest extends FlatSpec with Matchers {

  "A view" should "be instantiable without new" in {
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrandView should not be null
  }

  it should "have a name which is based on its class name and partitioning suffixes" in {
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrandView.n shouldEqual "product_brand_ec0106"
  }

  it should "have a module name which is defined by its package" in {
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrandView.module shouldEqual "test_eci_datahub"
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

    productBrandView.ecShopCode.n shouldEqual "ec_shop_code"
  }

  it should "be able to assign values to parameters" in {
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrandView.ecNr.v shouldEqual (Some("ec0106"))
  }

  it should "be able to read values from parameters explicitly" in {
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    val ecShopCodeRead = productBrandView.ecNr.v.get

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

  it should "collect all fields" in {
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))
    val fields = productBrandView.fields.map { f => (f.n, f.t) }

    fields should contain allOf (("product_id", manifest[String]), ("brand_name", manifest[String]), ("occurred_at", manifest[String]), ("created_at", manifest[Date]), ("created_by", manifest[String]))
  }

  it should "collect all parameters" in {
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))
    val parameterTypes = productBrandView.parameters.map { f => (f.n, f.t) }
    val parameterValues = productBrandView.parameters.map { f => (f.n, f.v) }
    parameterTypes should contain allOf (("ec_nr", manifest[String]), ("year", manifest[String]), ("month", manifest[String]), ("day", manifest[String]))
    parameterValues should contain allOf (("ec_nr", Some("ec0106")), ("year", Some("2014")), ("month", Some("01")), ("day", Some("01")))
  }

  "A view's field types" should "be accessible at both compile and runtime" in {
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrandView.occurredAt.t shouldBe manifest[String]
  }

  "A view's parameter types" should "be accessible at both compile and runtime" in {
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrandView.ecShopCode.t shouldBe manifest[String]
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

    productBrand.isPartition(productBrand.ecNr) shouldBe true
    productBrand.isPartition(productBrand.year) shouldBe true
    productBrand.isPartition(productBrand.month) shouldBe true
    productBrand.isPartition(productBrand.day) shouldBe true
  }

  "asSuffix" should "partition a view but as a suffix" in {
    val productBrand = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrand.isSuffixPartition(productBrand.ecNr) shouldBe true
    productBrand.isSuffixPartition(productBrand.year) shouldBe false
    productBrand.isSuffixPartition(productBrand.month) shouldBe false
    productBrand.isSuffixPartition(productBrand.day) shouldBe false
  }

  "privacySensitive" should "make a field privacy sensitive" in {
    val productBrand = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrand.brandName.isPrivacySensitive shouldBe true
    productBrand.productId.isPrivacySensitive shouldBe false
  }

  it should "also make partitions privacy sensitive" in {
    val productBrand = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    productBrand.ecNr.isPrivacySensitive shouldBe true
    productBrand.year.isPrivacySensitive shouldBe false
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

    productBrand.fields.map { _.n } should contain inOrder ("occurred_at", "product_id", "brand_name", "created_at", "created_by")
    productBrand.brand().fields.map { _.n } should contain inOrder ("id", "name", "created_at", "created_by")
    productBrand.product().fields.map { _.n } should contain inOrder ("id", "occurred_at", "name", "brand_id", "created_at", "created_by")

  }

  "A view" should "be transformable into DDL" in {
    val productBrand = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))
    val avro = AvroView(p("ec0106"), p("2014"), p("01"), p("01"))
    val edgeCases = EdgeCasesView()

    ddl(productBrand).contains("CREATE EXTERNAL TABLE IF NOT EXISTS dev_test_eci_datahub.product_brand_ec0106 (") shouldBe true
    ddl(productBrand).contains("occurred_at STRING,") shouldBe true
    ddl(productBrand).contains("ec_shop_code STRING,") shouldBe true
    ddl(productBrand).contains("product_id STRING,") shouldBe true
    ddl(productBrand).contains("brand_name STRING,") shouldBe true
    ddl(productBrand).contains("created_at STRING,") shouldBe true
    ddl(productBrand).contains("created_by STRING") shouldBe true
    ddl(productBrand).contains(")") shouldBe true
    ddl(productBrand).contains("COMMENT 'ProductBrand joins brands with products'") shouldBe true
    ddl(productBrand).contains("PARTITIONED BY (year STRING, month STRING, day STRING, date_id STRING)") shouldBe true
    ddl(productBrand).contains("STORED AS PARQUET") shouldBe true
    ddl(productBrand).contains("LOCATION '/hdp/dev/test/eci/datahub/product_brand_ec0106'") shouldBe true
  }

  it should "be transformable into DDL for an env" in {
    val productBrand = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))
    productBrand.env = "prod"

    ddl(productBrand).contains("CREATE EXTERNAL TABLE IF NOT EXISTS prod_test_eci_datahub.product_brand_ec0106 (") shouldBe true
    ddl(productBrand).contains("occurred_at STRING,") shouldBe true
    ddl(productBrand).contains("ec_shop_code STRING,") shouldBe true
    ddl(productBrand).contains("product_id STRING,") shouldBe true
    ddl(productBrand).contains("brand_name STRING,") shouldBe true
    ddl(productBrand).contains("created_at STRING,") shouldBe true
    ddl(productBrand).contains("created_by STRING") shouldBe true
    ddl(productBrand).contains(")") shouldBe true
    ddl(productBrand).contains("COMMENT 'ProductBrand joins brands with products'") shouldBe true
    ddl(productBrand).contains("PARTITIONED BY (year STRING, month STRING, day STRING, date_id STRING)") shouldBe true
    ddl(productBrand).contains("STORED AS PARQUET") shouldBe true
    ddl(productBrand).contains("LOCATION '/hdp/prod/test/eci/datahub/product_brand_ec0106'") shouldBe true

  }

  it should "inherit its env to its dependencies" in {
    val productBrand = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))
    productBrand.env = "prod"

    for (d <- productBrand.dependencies) {
      d.env shouldEqual productBrand.env
    }
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
    val productBrandView = View.newView(classOf[ProductBrand], "dev", Parameter.asParameter("ec0106"), Parameter.asParameter("2014"), Parameter.asParameter("01"), Parameter.asParameter("01"))

    productBrandView shouldEqual ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))
  }

  it should "be dynamically instantiatable via URL path" in {
    val views = View.viewsFromUrl("dev", "/test.eci.datahub/Product/e(EC0106,EC0101)/rymd(20140224-20131202)/")

    views.length shouldBe 2 * 85

    views.foreach {
      v =>
        val product = v.asInstanceOf[Product]
        val dateString = s"${product.year.v.get}${product.month.v.get}${product.day.v.get}"

        product.ecNr.v.get should (equal("EC0106") or equal("EC0101"))
        dateString should be <= "20140224"
        dateString should be >= "20131202"
    }
  }

  it should "be queryable" in {
    val views = View.viewsInPackage("test.eci.datahub")

    views should contain allOf (classOf[Brand], classOf[Product], classOf[ProductBrand], classOf[EdgeCasesView], classOf[AvroView], classOf[ViewWithDefaultParams], classOf[Click], classOf[ClickOfEC0101], classOf[ClickOfEC0101ViaOozie])

    val traits = View.getTraits(classOf[ProductBrand])

    traits should contain theSameElementsAs List(classOf[DailyParameterization], classOf[PointOccurrence], classOf[JobMetadata])
  }
}