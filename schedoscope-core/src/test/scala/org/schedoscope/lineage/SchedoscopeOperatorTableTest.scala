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

import java.util

import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.{SqlIdentifier, SqlOperator}
import org.scalatest.{FlatSpec, Matchers}
import org.schedoscope.dsl.Parameter._
import org.schedoscope.dsl.views.DateParameterizationUtils.today
import test.views.ProductBrand

import scala.collection.JavaConverters._

/**
  * @author Jan Hicken (jhicken)
  */
class SchedoscopeOperatorTableTest extends FlatSpec with Matchers {
  "A Schedoscope operator table" should "return an operator for a configured metastore function" in {
    val operatorTable = new SchedoscopeOperatorTable(ProductBrand(p("EC0101"), today._1, today._2, today._3))
    val list = new util.ArrayList[SqlOperator]()

    operatorTable.lookupOperatorOverloads(new SqlIdentifier("soundex", SqlParserPos.ZERO), null, null, list)
    list.asScala shouldBe 'nonEmpty
  }
}
