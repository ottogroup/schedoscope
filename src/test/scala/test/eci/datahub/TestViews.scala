package test.eci.datahub

import java.util.Date
import com.ottogroup.bi.soda.dsl.Avro
import org.schedoscope.dsl.Parameter
import com.ottogroup.bi.soda.dsl.Parameter.p
import com.ottogroup.bi.soda.dsl.Parquet
import org.schedoscope.dsl.Structure
import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.dsl.transformations.OozieTransformation
import com.ottogroup.bi.soda.dsl.transformations.OozieTransformation.oozieWFPath
import com.ottogroup.bi.soda.dsl.transformations.HiveTransformation
import com.ottogroup.bi.soda.dsl.transformations.HiveTransformation.insertInto
import com.ottogroup.bi.soda.dsl.views.DailyParameterization
import com.ottogroup.bi.soda.dsl.views.Id
import com.ottogroup.bi.soda.dsl.views.JobMetadata
import com.ottogroup.bi.soda.dsl.views.PointOccurrence
import com.ottogroup.bi.soda.dsl.transformations.MorphlineTransformation
import com.ottogroup.bi.soda.dsl.ExternalTextFile
import com.ottogroup.bi.soda.dsl.TextFile
import com.ottogroup.bi.soda.dsl.Redis
import com.ottogroup.bi.soda.Settings
import org.apache.hadoop.security.UserGroupInformation
import com.ottogroup.bi.soda.dsl.JDBC
import scala.io.Source
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

  transformVia(
    () => HiveTransformation(
      insertInto(this, s""" 
            SELECT ${click().id.n}, ${click().url.n} 
            FROM ${click().tableName} 
            WHERE ${click().ecShopCode.n} = '${click().ecShopCode.v.get}'""")))
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
      oozieWFPath(env, "bundle", "click"),
      Map()))
}

case class SimpleDependendView() extends View with Id {
  val field1 = fieldOf[String]
  locationPathBuilder = s => "src/test/resources/input"

  storedAs(TextFile())

}

case class MorphlineView() extends View with Id {
  val field1 = fieldOf[String]
  dependsOn(() => SimpleDependendView())
  transformVia(() => MorphlineTransformation(s"""{ id :"bla"
      importCommands : ["org.kitesdk.**"]
		  commands : [ {
		  				if  { 
                                          conditions: [{ not: {equals {ec_shop_code : ["${field1.n}"]}}}]
    								      then : [{ dropRecord{} }]
    										}}]}""").forView(this))
  locationPathBuilder = s => "src/test/resources/morphline.csv"
  storedAs(ExternalTextFile())
}

case class CompilingMorphlineView() extends View with Id {
  val visit_id = fieldOf[String]
  val site = fieldOf[String]
  dependsOn(() => SimpleDependendView())

  transformVia(() => MorphlineTransformation(s"""{ id :"bla"
      importCommands : ["org.kitesdk.**"]
		  commands : [ { extractAvroTree{} }
		  				]}""").forView(this))
  locationPathBuilder = s => "src/test/resources/compling_morphline.csv"
  storedAs(ExternalTextFile())
}

case class FailingMorphlineView() extends View with Id {
  dependsOn(() => SimpleDependendView())
  transformVia(() => MorphlineTransformation("invalid morphline code").forView(this))
  locationPathBuilder = s => "src/test/resources/failing_morphline.csv"
  storedAs(ExternalTextFile())

}

case class RedisMorphlineView() extends View with Id {
  val field1 = fieldOf[String]
  dependsOn(() => SimpleDependendView())
  transformVia(() => MorphlineTransformation().forView(this))
  storedAs(Redis(host = "localhost", port = 6379))

}

case class HDFSInputView() extends View with Id {
  val field1 = fieldOf[String]
  locationPathBuilder = s => "/tmp/test"
  storedAs(Parquet())

}

case class BlaMorphlineView(x: Parameter[String]) extends View {
  val visit_id = fieldOf[String]
  val site = fieldOf[String]
  val search_term = fieldOf[String]
  val number_of_results = fieldOf[String]
  dependsOn(() => HDFSInputView())

  transformVia(() => MorphlineTransformation(s"""{ id :"bla"
          importCommands : ["org.kitesdk.**"]
		  commands : [ {extractAvroPaths{ flatten :true 
                                         paths :{visit_id : "/visit_id"
		  										 site : "/ec_shop_code"
		  										 number_of_results : /number_of_results
		  										 search_term : /search_term
		  										} 
		  								}} ,
                        {
		  				if  { 
                                          conditions: [{ not: {equals {site : "${x.v.get}"}}}]
    								      then : [{ dropRecord{} }]
    										}}]}""").forView(this))
  locationPathBuilder = s => "src/test/resources/bla_morphline.csv"
  storedAs(ExternalTextFile())
}

case class JDBCMorphlineView(x: Parameter[String]) extends View {
  val visit_id = fieldOf[String]
  val site = fieldOf[String]
  val search_term = fieldOf[String]
  val number_of_results = fieldOf[Integer]
  val has_result = fieldOf[Boolean]
  dependsOn(() => HDFSInputView())

  transformVia(() => MorphlineTransformation(s"""{ id :"bla"
          importCommands : ["org.kitesdk.**"]
		  commands : [ {extractAvroPaths{ flatten :true 
                                         paths :{visit_id : "/visit_id"
		  										 site : "/ec_shop_code"
		  										 number_of_results : /number_of_results
		  										 search_term : /search_term
		  										} 
		  								}} ,
                        {
		  				if  { 
                                          conditions: [{ not: {equals {number_of_results : "0"}}}]
    								      then : [{ addValues { has_result : true} }]
    										}}]}""").forView(this))

  storedAs(JDBC(jdbcUrl = "jdbc:exa:127.0.0.1:8563;schema=test", userName = "test", password = "test", jdbcDriver = "com.exasol.jdbc.EXADriver"))
}