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
package test.views

import org.apache.hadoop.hive.ql.udf.generic.GenericUDFSoundex
import org.schedoscope.dsl.Parameter._
import org.schedoscope.dsl.storageformats.{Avro, Parquet, TextFile, _}
import org.schedoscope.dsl.transformations.Export._
import org.schedoscope.dsl.transformations.HiveTransformation
import org.schedoscope.dsl.transformations.HiveTransformation._
import org.schedoscope.dsl.views._
import org.schedoscope.dsl.{Parameter, Structure, View}
import org.schedoscope.export.testsupport.EmbeddedFtpSftpServer
import test.extviews.ExternalShop

import scala.util.Random

case class Brand(shopCode: Parameter[String]) extends View
  with Id
  with JobMetadata {

  comment("In this example, brands are per shop but time invariant")

  val name = fieldOf[String]("The brand's name, but field name overriden", "brand_name")

  asTableSuffix(shopCode)
}

case class Product(shopCode: Parameter[String],
                   year: Parameter[String],
                   month: Parameter[String],
                   day: Parameter[String]) extends View
  with Id
  with PointOccurrence
  with JobMetadata
  with DailyParameterization {

  comment("In this example, shops have different products each day")

  val name = fieldOf[String]
  val brandId = fieldOf[String]

  asTableSuffix(shopCode)
}

case class ProductBrand(shopCode: Parameter[String],
                        year: Parameter[String],
                        month: Parameter[String],
                        day: Parameter[String]) extends View
  with PointOccurrence
  with JobMetadata
  with DailyParameterization {

  comment("ProductBrand joins brands with products")

  val productId = fieldOf[String]
  val brandName = fieldOf[String]

  val brand = dependsOn(() => Brand(shopCode))
  val product = dependsOn(() => Product(shopCode, year, month, day))

  storedAs(Parquet())

  transformVia(() =>
    HiveTransformation(insertInto(
      this,
      s"""
         SELECT
          		p.${product().occurredAt.n} AS ${this.occurredAt.n},
      				p.${product().id.n} AS ${this.productId.n},
          		b.${brand().name.n} AS ${this.brandName.n},
          		'some date time' AS ${this.createdAt.n},
          		'${"ProductBrand"}' AS ${this.createdBy.n}
          FROM 		${product().tableName} p
          JOIN 		${brand().tableName} b
          ON		p.${product().brandId.n} = b.${brand().id.n}
          WHERE 	p.${product().year.n} = "${this.year.v.get}"
          AND 		p.${product().month.n} = "${this.month.v.get}"
          AND 		p.${product().day.n} = "${this.day.v.get}"
          """),
      withFunctions(this, Map("soundex" -> classOf[GenericUDFSoundex]))))
}

case class ViewWithIllegalExternalDeps(shopCode: Parameter[String]) extends View {

  val shop = dependsOn(() => external(Brand(shopCode)))

  val productId = fieldOf[String]
  val productName = fieldOf[String]

  transformVia(() =>
    HiveTransformation(insertInto(
      this,
      s"""SELECT * FROM ${shop().n}""")))
}

case class ViewWithIllegalInternalDeps(shopCode: Parameter[String]) extends View {

  val shop = dependsOn(() => ExternalShop())

  val productId = fieldOf[String]
  val productName = fieldOf[String]

  transformVia(() =>
    HiveTransformation(insertInto(
      this,
      s"""SELECT * FROM ${shop().n}""")))
}

case class ViewWithExternalDeps(shopCode: Parameter[String],
                                year: Parameter[String],
                                month: Parameter[String],
                                day: Parameter[String]) extends View
  with PointOccurrence
  with JobMetadata
  with DailyParameterization {

  val shop = dependsOn(() => external(ExternalShop()))

  val productId = fieldOf[String]
  val productName = fieldOf[String]

  transformVia(() =>
    HiveTransformation(insertInto(
      this,
      s"""SELECT * FROM ${shop().n}""")))
}

case class ViewWithExceptionThrowingDependency() extends View {
  dependsOn(() => ViewThrowingExceptionUponInitialization())
}

case class ViewThrowingExceptionUponInitialization() extends View {
  throw new IllegalArgumentException("This view will not initialize")
}

trait Shop {
  val shopCode: Parameter[String]
  require((shopCode.v.get).toUpperCase().equals(shopCode.v.get), "Put in upper case: " + shopCode.v.get)
}

case class ProductBrandMaterializeOnce(shopCode: Parameter[String],
                                       year: Parameter[String],
                                       month: Parameter[String],
                                       day: Parameter[String]) extends View
  with PointOccurrence
  with JobMetadata
  with DailyParameterization {

  comment("ProductBrand joins brands with products")

  val productId = fieldOf[String]
  val brandName = privacySensitive(fieldOf[String])

  val brand = dependsOn(() => Brand(shopCode))
  val product = dependsOn(() => Product(shopCode, year, month, day))

  storedAs(Parquet())
  materializeOnce

  transformVia(() =>
    HiveTransformation(insertInto(
      this,
      s"""
         SELECT
      				p.${product().id.n} AS ${this.productId.n},
          		b.${brand().name.n} AS ${this.brandName.n},
          		p.${product().occurredAt.n} AS ${this.occurredAt.n}
          		'some date time' AS ${this.createdAt.n}
          		${"ProductBrand"} AS ${this.createdBy.n}
          FROM 		${product().n} p
          JOIN 		${brand().n} b
          ON		p.${product().brandId.n} = b.${brand().id.n}
          WHERE 	p.${product().year.n} = ${this.year.v.get}
          AND 		p.${product().month.n} = ${this.month.v.get}
          AND 		p.${product().day.n} = ${this.day.v.get}
          """)))
}

case class ProductBrandsNoOpMirror(year: Parameter[String],
                                   month: Parameter[String],
                                   day: Parameter[String]) extends View {

  dependsOn(() => ProductBrand(p("EC0101"), year, month, day))
  dependsOn(() => ProductBrand(p("EC0102"), year, month, day))
}

case class ProductBrandsNoOpMirrorDependent(
                                             year: Parameter[String],
                                             month: Parameter[String],
                                             day: Parameter[String]) extends View {

  dependsOn(() => ProductBrandsNoOpMirror(year, month, day))
}

case class NestedStructure() extends Structure {
  val aField = fieldOf[Boolean]
}

case class ComplexStructure() extends Structure {
  val aField = fieldOf[Int]
  val aComplexField = fieldOf[List[NestedStructure]]
}

case class EdgeCasesView() extends View {
  val aMap = fieldOf[Map[String, Int]]
  val anArray = fieldOf[List[Int]]
  val aComplicatedBitch = fieldOf[Map[List[String], List[Map[String, Int]]]]
  val aStructure = fieldOf[ComplexStructure]
}

case class AvroView(
                     year: Parameter[String],
                     month: Parameter[String],
                     day: Parameter[String]) extends View
  with DailyParameterization {

  val aField = fieldOf[String]
  val anotherField = fieldOf[String]

  storedAs(Avro("test.avsc"))
}

case class ViewWithDefaultParams(year: Parameter[String],
                                 month: Parameter[String],
                                 day: Parameter[String],
                                 defaultParameter: Int = 2) extends View {
}

case class Click(shopCode: Parameter[String],
                 year: Parameter[String],
                 month: Parameter[String],
                 day: Parameter[String]) extends View
  with Id
  with DailyParameterization {

  val url = fieldOf[String]
}

trait ClickEC01 extends View
  with Id
  with DailyParameterization {

  val url = fieldOf[String]
  val click = dependsOn(() => Click(p("EC0101"), year, month, day))

  transformVia(
    () => HiveTransformation(
      insertInto(this,
        s"""
            SELECT ${click().id.n}, ${click().url.n}
            FROM ${click().tableName}
            WHERE ${click().shopCode.n} = '${click().shopCode.v.get}'""")))
}

case class ClickOfEC01(year: Parameter[String],
                     month: Parameter[String],
                     day: Parameter[String]) extends ClickEC01
   {
  storedAs(TextFile())
}

case class ClickEC01Json(year: Parameter[String],
                         month: Parameter[String],
                         day: Parameter[String]) extends ClickEC01
{
  storedAs(Json())
}

case class ClickEC01ORC(year: Parameter[String],
                        month: Parameter[String],
                        day: Parameter[String]) extends ClickEC01
{
  storedAs(OptimizedRowColumnar())
}

case class ClickEC01Parquet(year: Parameter[String],
                            month: Parameter[String],
                            day: Parameter[String]) extends ClickEC01
{
  storedAs(Parquet())
}

case class ClickEC01Avro(year: Parameter[String],
                         month: Parameter[String],
                         day: Parameter[String]) extends ClickEC01
{
  storedAs(Avro("avro_schemas/click_of_e_c0101_avro.avsc"))
}

case class ClickOfEC0101WithJdbcExport(year: Parameter[String],
                                     month: Parameter[String],
                                     day: Parameter[String]) extends View
  with Id
  with DailyParameterization {

  val url = fieldOf[String]

  val click = dependsOn(() => Click(p("EC0101"), year, month, day))

  transformVia(
    () => HiveTransformation(
      insertInto(this,
        s"""
            SELECT ${click().id.n}, ${click().url.n}
            FROM ${click().tableName}
            WHERE ${click().shopCode.n} = '${click().shopCode.v.get}'""")))

  exportTo(() => Jdbc(this, "jdbc:derby:memory:TestingDB"))

}

case class ClickOfEC0101WithRedisExport(year: Parameter[String],
                                      month: Parameter[String],
                                      day: Parameter[String]) extends View
  with Id
  with DailyParameterization {

  val url = fieldOf[String]

  val click = dependsOn(() => Click(p("EC0101"), year, month, day))

  transformVia(
    () => HiveTransformation(
      insertInto(this,
        s"""
            SELECT ${click().id.n}, ${click().url.n}
            FROM ${click().tableName}
            WHERE ${click().shopCode.n} = '${click().shopCode.v.get}'""")))

  exportTo(() => Redis(this, "localhost", id))

}

case class ClickOfEC01WithKafkaExport(year: Parameter[String],
                                      month: Parameter[String],
                                      day: Parameter[String]) extends View
  with Id
  with DailyParameterization {

  val url = fieldOf[String]

  val click = dependsOn(() => Click(p("EC0101"), year, month, day))

  transformVia(
    () => HiveTransformation(
      insertInto(this,
        s"""
            SELECT ${click().id.n}, ${click().url.n}
            FROM ${click().tableName}
            WHERE ${click().shopCode.n} = '${click().shopCode.v.get}'""")))

  exportTo(() => Kafka(this, id, "localhost:9092", "localhost:2182"))

}

case class ClickOfEC0101WithFtpExport(year: Parameter[String],
                                    month: Parameter[String],
                                    day: Parameter[String]) extends View
  with Id
  with DailyParameterization {

  val url = fieldOf[String]

  val click = dependsOn(() => Click(p("EC0101"), year, month, day))

  transformVia(
    () => HiveTransformation(
      insertInto(this,
        s"""
            SELECT ${click().id.n}, ${click().url.n}
            FROM ${click().tableName}
            WHERE ${click().shopCode.n} = '${click().shopCode.v.get}'""")))

  val filePrefix = Random.alphanumeric.take(10).mkString

  exportTo(() => Ftp(this,
    "ftp://localhost:2221/",
    EmbeddedFtpSftpServer.FTP_USER_FOR_TESTING,
    EmbeddedFtpSftpServer.FTP_PASS_FOR_TESTING,
    filePrefix))
}

case class ClicksGroupUrlShop(shopCodes: Parameter[List[String]],
                              year: Parameter[String],
                              month: Parameter[String],
                              day: Parameter[String]) extends View
  with DailyParameterization {

  val url = fieldOf[String]
  val clickCount = fieldOf[Long]

  dependsOn(() =>
    for (shopCode <- shopCodes.v.get)
      yield Click(p(shopCode), year, month, day))

  val click = Click(p(shopCodes.v.get.head), year, month, day)

  transformVia(
    () => HiveTransformation(
      insertInto(this,
        s"""
           SELECT ${click.url.n}, MIN(${click.id.n})
           FROM ${click.tableName}
           GROUP BY ${click.url.n}, ${click.shopCode.n}
         """)
    )
  )
}



case class SimpleDependendView() extends View with Id {
  val field1 = fieldOf[String]
  tablePathBuilder = s => "src/test/resources/input"

  storedAs(TextFile())

}

case class HDFSInputView() extends View with Id {
  val field1 = fieldOf[String]
  tablePathBuilder = s => "/tmp/test"
  storedAs(Parquet())
}

case class RequireView(shopCode: Parameter[String])
  extends View with Shop {

  val field1 = fieldOf[String]
}

case class ArticleViewParquet() extends View {
  val name = fieldOf[String]
  val number = fieldOf[Int]

  tblProperties(Map("orc.compress" -> "ZLIB", "transactional" -> "true"))
  storedAs(Parquet())
}

case class ArticleViewParquet2() extends View {
  val name = fieldOf[String]
  val number = fieldOf[Int]

  storedAs(Parquet(fullRowFormatCreateTblStmt = true))
}

case class ArticleViewSequence() extends View {
  val name = fieldOf[String]
  val number = fieldOf[Int]

  storedAs(SequenceFile())
}

case class ArticleViewAvro() extends View {
  val name = fieldOf[String]
  val number = fieldOf[Int]

  tblProperties(Map("immutable" -> "true"))
  storedAs(Avro("myPath"))
}

case class ArticleViewAvro2() extends View {
  val name = fieldOf[String]
  val number = fieldOf[Int]

  tblProperties(Map("immutable" -> "true"))
  storedAs(Avro("myPath", fullRowFormatCreateTblStmt = false))
}

case class ArticleViewOrc() extends View {
  val name = fieldOf[String]
  val number = fieldOf[Int]

  tblProperties(Map("immutable" -> "false"))
  storedAs(OptimizedRowColumnar())
}

case class ArticleViewOrc2() extends View {
  val name = fieldOf[String]
  val number = fieldOf[Int]

  storedAs(OptimizedRowColumnar(fullRowFormatCreateTblStmt = true))
}

case class ArticleViewRc() extends View {
  val name = fieldOf[String]
  val number = fieldOf[Int]

  tblProperties(Map("scalable" -> "true"))
  storedAs(RecordColumnarFile())
}

case class ArticleViewCsv() extends View {
  val name = fieldOf[String]
  val number = fieldOf[Int]

  storedAs(Csv(serDeProperties = Map("separatorChar" ->"""\t""",
    "quoteChar" -> "'", "escapeChar" ->"""\\""")))
}

case class ArticleViewRegEx() extends View {
  val name = fieldOf[String]
  val number = fieldOf[Int]

  storedAs(TextFile(serDe = "org.apache.hadoop.hive.serde2.RegexSerDe",
    serDeProperties = Map("input.regex" -> "test")))
}

case class ArticleViewJson() extends View {
  val name = fieldOf[String]
  val number = fieldOf[Int]

  tblProperties(Map("transactional" -> "true"))
  storedAs(Json())
}

case class ArticleViewTextFile1() extends View {
  val name = fieldOf[String]
  val number = fieldOf[Int]

  tblProperties(Map("what" -> "ever"))
  storedAs(TextFile(fieldTerminator = """\\001""",
    collectionItemTerminator = """\002""",
    mapKeyTerminator = """\003""",
    lineTerminator = """\n"""
  ))
}

case class ArticleViewTextFile2() extends View {
  val name = fieldOf[String]
  val number = fieldOf[Int]

  tblProperties(Map("what" -> "buh"))
  storedAs(TextFile(serDe = "org.apache.hadoop.hive.serde2.OpenCSVSerde",
    serDeProperties = Map("separatorChar" ->"""\t""",
      "escapeChar" ->"""\\""")))
}

case class ArticleViewTextFile3() extends View {
  val name = fieldOf[String]
  val number = fieldOf[Int]

  tblProperties(Map("what" -> "buh"))
  storedAs(TextFile(fullRowFormatCreateTblStmt = true))
}

case class ArticleViewInOutput() extends View {
  val name = fieldOf[String]
  val number = fieldOf[Int]

  tblProperties(Map("EXTERNAL" -> "TRUE"))
  storedAs(InOutputFormat(input = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
    output = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
    serDe = "org.apache.hadoop.hive.ql.io.orc.OrcSerde"))
}

case class ArticleViewS3() extends View {
  val name = fieldOf[String]
  val number = fieldOf[Int]

  tblProperties(Map("orc.compress" -> "ZLIB", "transactional" -> "true"))
  storedAs(S3("schedoscope-bucket-test", OptimizedRowColumnar(), "s3a"))
}
