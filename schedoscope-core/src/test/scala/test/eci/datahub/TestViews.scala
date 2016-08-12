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
package test.eci.datahub

import java.util.Date
import org.schedoscope.export.testsupport.EmbeddedFtpSftpServer
import org.schedoscope.dsl.Parameter
import org.schedoscope.dsl.Parameter.p
import org.schedoscope.dsl.Structure
import org.schedoscope.dsl.View
import org.schedoscope.dsl.transformations.OozieTransformation
import org.schedoscope.dsl.transformations.OozieTransformation.oozieWFPath
import org.schedoscope.dsl.transformations.HiveTransformation
import org.schedoscope.dsl.transformations.Touch
import org.schedoscope.dsl.transformations.Export._
import org.schedoscope.dsl.transformations.HiveTransformation.insertInto
import org.schedoscope.dsl.views.DailyParameterization
import org.schedoscope.dsl.views.Id
import org.schedoscope.dsl.views.JobMetadata
import org.schedoscope.dsl.views.PointOccurrence
import org.schedoscope.Settings
import org.apache.hadoop.security.UserGroupInformation
import org.schedoscope.dsl.storageformats._
import scala.io.Source
import scala.util.Random

case class Brand(
  shopCode: Parameter[String]) extends View
    with Id
    with JobMetadata {

  comment("In this example, brands are per shop but time invariant")

  val name = fieldOf[String]("The brand's name, but field name overriden", "brand_name")

  asTableSuffix(shopCode)
}

case class Product(
  shopCode: Parameter[String],
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

case class ProductBrand(
  shopCode: Parameter[String],
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

case class ProductBrandMaterializeOnce(
  shopCode: Parameter[String],
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

case class ProductBrandsNoOpMirror(
    year: Parameter[String],
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

case class ViewWithDefaultParams(
    year: Parameter[String],
    month: Parameter[String],
    day: Parameter[String],
    defaultParameter: Int = 2) extends View {
}

case class Click(
  shopCode: Parameter[String],
  year: Parameter[String],
  month: Parameter[String],
  day: Parameter[String]) extends View
    with Id
    with DailyParameterization {

  val url = fieldOf[String]
}

case class ClickOfEC0101(
  year: Parameter[String],
  month: Parameter[String],
  day: Parameter[String]) extends View
    with Id
    with DailyParameterization {

  val url = fieldOf[String]

  val click = dependsOn(() => Click(p("EC0101"), year, month, day))

  transformVia(
    () => HiveTransformation(
      insertInto(this, s"""
            SELECT ${click().id.n}, ${click().url.n}
            FROM ${click().tableName}
            WHERE ${click().shopCode.n} = '${click().shopCode.v.get}'""")))
}

case class ClickOfEC0101WithJdbcExport(
  year: Parameter[String],
  month: Parameter[String],
  day: Parameter[String]) extends View
    with Id
    with DailyParameterization {

  val url = fieldOf[String]

  val click = dependsOn(() => Click(p("EC0101"), year, month, day))

  transformVia(
    () => HiveTransformation(
      insertInto(this, s"""
            SELECT ${click().id.n}, ${click().url.n}
            FROM ${click().tableName}
            WHERE ${click().shopCode.n} = '${click().shopCode.v.get}'""")))

  exportTo(() => Jdbc(this, "jdbc:derby:memory:TestingDB"))

}

case class ClickOfEC0101WithRedisExport(
  year: Parameter[String],
  month: Parameter[String],
  day: Parameter[String]) extends View
    with Id
    with DailyParameterization {

  val url = fieldOf[String]

  val click = dependsOn(() => Click(p("EC0101"), year, month, day))

  transformVia(
    () => HiveTransformation(
      insertInto(this, s"""
            SELECT ${click().id.n}, ${click().url.n}
            FROM ${click().tableName}
            WHERE ${click().shopCode.n} = '${click().shopCode.v.get}'""")))

  exportTo(() => Redis(this, "localhost", id))

}

case class ClickOfEC0101WithKafkaExport(
  year: Parameter[String],
  month: Parameter[String],
  day: Parameter[String]) extends View
    with Id
    with DailyParameterization {

  val url = fieldOf[String]

  val click = dependsOn(() => Click(p("EC0101"), year, month, day))

  transformVia(
    () => HiveTransformation(
      insertInto(this, s"""
            SELECT ${click().id.n}, ${click().url.n}
            FROM ${click().tableName}
            WHERE ${click().shopCode.n} = '${click().shopCode.v.get}'""")))

  exportTo(() => Kafka(this, id, "localhost:9092", "localhost:2182"))

}

case class ClickOfEC0101WithFtpExport(
  year: Parameter[String],
  month: Parameter[String],
  day: Parameter[String]) extends View
    with Id
    with DailyParameterization {

  val url = fieldOf[String]

  val click = dependsOn(() => Click(p("EC0101"), year, month, day))

  transformVia(
    () => HiveTransformation(
      insertInto(this, s"""
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

case class ClickOfEC0101ViaOozie(
  year: Parameter[String],
  month: Parameter[String],
  day: Parameter[String]) extends View
    with Id
    with DailyParameterization {

  val url = fieldOf[String]

  val click = dependsOn(() => Click(p("EC0101"), year, month, day))

  transformVia(
    () => OozieTransformation(
      "bundle", "click",
      oozieWFPath("bundle", "click")))
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

trait Shop {
  val shopCode: Parameter[String]
  require((shopCode.v.get).toUpperCase().equals(shopCode.v.get), "Put in upper case: "+ shopCode.v.get)
}

case class RequireView(shopCode: Parameter[String])
  extends View with Shop {

  val field1 = fieldOf[String]
}

