/*
 * Copyright 2016 Otto (GmbH & Co KG)
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

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}
import org.schedoscope.dsl.Parameter.p
import org.schedoscope.dsl.views.DateParameterizationUtils.today
import test.views.{ClickOfEC0101, ProductBrand}

/**
  * @author Jan Hicken (jhicken)
  */
class DependencyAnalyzerTest extends FlatSpec with Matchers with PrivateMethodTester with TableDrivenPropertyChecks {
  private val preprocessSql = PrivateMethod[String]('preprocessSql)

  "The dependency analyzer" should "analyze lineage for ProductBrand correctly" in {
    val v = ProductBrand(p("EC0101"), today._1, today._2, today._3)

    DependencyAnalyzer.analyzeLineage(v, recurse = true) shouldEqual Map(
      v.occurredAt → Set(v.product().occurredAt),
      v.productId → Set(v.product().id),
      v.brandName → Set(v.brand().name),
      v.createdAt → Set(),
      v.createdBy → Set()
    )
  }

  it should "analyze dependencies for ProductBrand correctly" in {
    val v = ProductBrand(p("EC0201"), today._1, today._2, today._3)

    DependencyAnalyzer.analyzeDependencies(v, recurse = true) shouldEqual Map(
      v.occurredAt → Set(v.product().occurredAt, v.product().year, v.product().month, v.product().day,
        v.product().brandId, v.brand().id),
      v.productId → Set(v.product().id, v.product().year, v.product().month, v.product().day, v.product().brandId,
        v.brand().id),
      v.brandName → Set(v.brand().name, v.product().year, v.product().month, v.product().day, v.product().brandId,
        v.brand().id),
      v.createdAt → Set(),
      v.createdBy → Set()
    )
  }

  it should "analyze lineage for ClickOfEC0101 correctly" in {
    val v = ClickOfEC0101(today._1, today._2, today._3)

    DependencyAnalyzer.analyzeLineage(v, recurse = true) shouldEqual Map(
      v.id → Set(v.click().id),
      v.url → Set(v.click().url)
    )
  }

  it should "analyze dependencies for ClickOfEC0101 correctly" in {
    val v = ClickOfEC0101(today._1, today._2, today._3)

    DependencyAnalyzer.analyzeDependencies(v, recurse = true) shouldEqual Map(
      v.id → Set(v.click().id, v.click().shopCode),
      v.url → Set(v.click().url, v.click().shopCode)
    )
  }

  it should "pre-process SQL by emptying double quoted strings" in {
    val badSql = """regexp_replace(csv_line_to_array.field_array[0] , "\"", "")"""
    val goodSql = """regexp_replace(csv_line_to_array.field_array[0] , '', '')"""

    DependencyAnalyzer invokePrivate preprocessSql(badSql) shouldEqual goodSql
  }

  it should "pre-process SQL by emptying double quoted strings with single quotes in them" in {
    val badSql = """"[\\s,\;.:\\u00bb\\u00ab\"'\\u0060\\u00b4|<>\\-_!\\u00a7a%&/()=?{\\[\\]}\\\\]""""
    val goodSql = "''"

    DependencyAnalyzer invokePrivate preprocessSql(badSql) shouldEqual goodSql
  }

  it should "pre-process SQL by emptying strings with escaped single quotes" in {
    val badSql = """unix_timestamp(last_value(wl.occurred_at) OVER session, 'yyyy-MM-dd\'T\'HH:mm:ss.SSSXXX')"""
    val goodSql = "unix_timestamp(last_value(wl.occurred_at) OVER session, '')"

    DependencyAnalyzer invokePrivate preprocessSql(badSql) shouldEqual goodSql
  }
}
