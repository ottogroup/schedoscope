package test.eci.datahub

import com.ottogroup.bi.soda.dsl.Parameter
import com.ottogroup.bi.soda.dsl.Parameter._
import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.dsl.views.Id
import com.ottogroup.bi.soda.dsl.views.JobMetadata
import com.ottogroup.bi.soda.dsl.views.PointOccurrence
import com.ottogroup.bi.soda.dsl.Parquet
import com.ottogroup.bi.soda.dsl.Structure
import java.util.Date
import com.ottogroup.bi.soda.dsl.Avro
import com.ottogroup.bi.soda.dsl.views.DailyParameterization
import com.ottogroup.bi.soda.dsl.transformations.sql.HiveTransformation
import com.ottogroup.bi.soda.dsl.transformations.sql.HiveTransformation._
import com.ottogroup.bi.soda.dsl.views.DailyParameterization

case class Brand(
  ecNr: Parameter[String]) extends View
  with Id
  with JobMetadata {

  comment("In this example, brands are per shop but time invariant")

  val ecShopCode = fieldOf[String]
  val name = fieldOf[String]

  asTableSuffix(ecNr)
}

case class Product(
  ecNr: Parameter[String],
  year: Parameter[String],
  month: Parameter[String],
  day: Parameter[String]) extends View
  with Id
  with PointOccurrence
  with JobMetadata
  with DailyParameterization {

  comment("In this example, shops have different products each day")

  val ecShopCode = fieldOf[String]
  val name = fieldOf[String]
  val brandId = fieldOf[String]

  asTableSuffix(ecNr)
}

case class ProductBrand(
  ecNr: Parameter[String],
  year: Parameter[String],
  month: Parameter[String],
  day: Parameter[String]) extends View
  with PointOccurrence
  with JobMetadata
  with DailyParameterization {

  comment("ProductBrand joins brands with products")

  val ecShopCode = fieldOf[String]
  val productId = fieldOf[String]
  val brandName = privacySensitive(fieldOf[String])

  val brand = dependsOn(() => Brand(ecNr))
  val product = dependsOn(() => Product(ecNr, year, month, day))

  asTableSuffix(privacySensitive(ecNr))
  storedAs(Parquet())

  transformVia(() =>
    HiveTransformation(insertInto(
      this,
      s"""
         SELECT 	${this.ecNr.v.get} AS ${this.ecShopCode.n},
      				p.${product().id.n} AS ${this.productId.n}, 
          			b.${brand().name.n} AS ${this.brandName.n}, 
          			p.${product().occurredAt.n} AS ${this.occurredAt.n}
          			${new Date} AS ${this.createdAt.n}
          			${"ProductBrand"} AS ${this.createdBy.n}
          FROM 		${product().n} p
          JOIN 		${brand().n} b
          ON		p.${product().brandId.n} = b.${brand().id.n}
          WHERE 	p.${product().year.n} = ${this.year.v.get}
          AND 		p.${product().month.n} = ${this.month.v.get}
          AND 		p.${product().day.n} = ${this.day.v.get}           
          """)))
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
  ecNr: Parameter[String],
  year: Parameter[String],
  month: Parameter[String],
  day: Parameter[String]) extends View
  with DailyParameterization {

  val aField = fieldOf[String]
  val anotherField = fieldOf[String]

  asTableSuffix(ecNr)

  storedAs(Avro("test.avsc"))
}

case class ViewWithDefaultParams(
  ecNr: Parameter[String],
  year: Parameter[String],
  month: Parameter[String],
  day: Parameter[String],
  defaultParameter: Int = 2) extends View {
}

case class Click(
  ecShopCode: Parameter[String],
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

  transformVia {
    () => HiveTransformation(insertInto(this, s"SELECT ${click().id.n}, ${click().url.n} FROM ${click().tableName} WHERE ${click().ecShopCode.n} = '${click().ecShopCode.v.get}'"))
  }
}