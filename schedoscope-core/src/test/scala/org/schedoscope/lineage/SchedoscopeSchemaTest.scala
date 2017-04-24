/*
 * Copyright 2017 Otto (GmbH & Co KG)
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

package org.schedoscope.lineage

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.schema.Schema
import org.apache.calcite.sql.`type`.SqlTypeFactoryImpl
import org.apache.calcite.sql.`type`.SqlTypeName.{BOOLEAN, INTEGER, VARCHAR}
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, Outcome, fixture}
import org.schedoscope.dsl.Parameter.p
import org.schedoscope.dsl.views.DateParameterizationUtils.today
import test.views.{EdgeCasesView, ProductBrand}

import scala.collection.JavaConverters._

/**
  * @author Jan Hicken (jhicken)
  */
class SchedoscopeSchemaTest extends fixture.FlatSpec with Matchers with TableDrivenPropertyChecks {

  case class FixtureParam(schema: Schema)

  val dataTypeFactory = new SqlTypeFactoryImpl(new HiveTypeSystemImpl)
  val productBrand = ProductBrand(p("EC0101"), today._1, today._2, today._3)
  val views = Set(
    productBrand, productBrand.brand(), productBrand.product(),
    EdgeCasesView()
  )

  override def withFixture(test: OneArgTest): Outcome = {
    val schema = SchedoscopeSchema(views)

    withFixture(test.toNoArgTest(FixtureParam(schema)))
  }

  val tables = Table(
    "table name" → "field names",
    "brand_ec0101" → Set("id", "created_at", "created_by", "brand_name"),
    "product_ec0101" → Set("id", "created_at", "created_by", "occurred_at", "year", "month", "day", "date_id", "name",
      "brand_id"),
    "product_brand" → Set("created_at", "created_by", "occurred_at", "year", "month", "day", "date_id", "product_id",
      "brand_name", "shop_code")
  )

  "A Schedoscope schema" should "return a table for all given views" in { f =>
    forAll(tables) { (tblName: String, _) =>
      f.schema.getTableNames.asScala should contain(tblName)
      f.schema.getTable(tblName) shouldNot be(null)
    }
  }

  it should "return the correct columns for each table" in { f =>
    forAll(tables) { (tblName: String, fieldNames: Traversable[String]) =>
      val rowType = f.schema.getTable(tblName).getRowType(dataTypeFactory)
      rowType.getFieldCount shouldEqual fieldNames.size
      for (fieldName <- fieldNames) {
        rowType.getFieldNames.asScala should contain(fieldName)
        rowType.getField(fieldName, false, false) shouldNot be(null)
      }
    }
  }

  val edgeCasesViewFields = Table(
    "field name" → "field type",
    "a_map" → dataTypeFactory.createMapType(
      dataTypeFactory.createSqlType(VARCHAR),
      dataTypeFactory.createSqlType(INTEGER)
    ),
    "an_array" → dataTypeFactory.createArrayType(
      dataTypeFactory.createSqlType(INTEGER), -1
    ),
    "a_complicated_bitch" → dataTypeFactory.createMapType(
      dataTypeFactory.createArrayType(
        dataTypeFactory.createSqlType(VARCHAR), -1
      ),
      dataTypeFactory.createArrayType(
        dataTypeFactory.createMapType(
          dataTypeFactory.createSqlType(VARCHAR),
          dataTypeFactory.createSqlType(INTEGER)
        ), -1)
    ),
    "a_structure" → dataTypeFactory.createStructType(
      List(
        dataTypeFactory.createSqlType(INTEGER),
        dataTypeFactory.createArrayType(
          dataTypeFactory.createStructType(
            List(
              dataTypeFactory.createSqlType(BOOLEAN)
            ).asJava,
            List("a_field").asJava
          ), -1
        )
      ).asJava,
      List("a_field", "a_complex_field").asJava
    )
  )

  it should "be able to deal with complex field types" in { f =>
    val rowType = f.schema.getTable("edge_cases_view").getRowType(dataTypeFactory)

    rowType.getFieldCount shouldEqual 4
    forAll(edgeCasesViewFields) { (fieldName: String, fieldType: RelDataType) =>
      rowType.getFieldNames should contain(fieldName)
      rowType.getField(fieldName, false, false) shouldNot be(null)
      rowType.getField(fieldName, false, false).getType shouldEqual fieldType
    }
  }
}
