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

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.prepare.PlannerImpl
import org.apache.calcite.rel.RelNode
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.validate.SqlValidatorImpl
import org.apache.calcite.sql2rel.{RelDecorrelator, SqlToRelConverter}
import org.apache.calcite.tools.{FrameworkConfig, ValidationException}
import org.slf4j.LoggerFactory

/**
  * @author Jan Hicken (jhicken)
  */
class NonFlatteningPlannerImpl(config: FrameworkConfig) extends PlannerImpl(config) {
  private val log = LoggerFactory.getLogger(getClass)
  private var validator: SqlValidatorImpl = _

  override def parse(sql: String): SqlNode = {
    log.debug("Parsing SQL:\n{}", sql)
    super.parse(sql)
  }

  override def validate(sqlNode: SqlNode): SqlNode = {
    validator = new IdentifierExpandingSqlValidator(getPrivateField("operatorTable"),
      invokePrivateMethod("createCatalogReader"), getPrivateField("typeFactory"))

    try {
      validator.validate(sqlNode)
    } catch {
      case ex: RuntimeException => throw new ValidationException(ex)
    }
  }

  override def convert(sql: SqlNode): RelNode = {
    val sqlToRelConverter = new SqlToRelConverter(new ViewExpanderImpl, validator,
      invokePrivateMethod("createCatalogReader"), getPrivateField("planner"), invokePrivateMethod("createRexBuilder"),
      getPrivateField("convertletTable"))

    sqlToRelConverter.setTrimUnusedFields(false)
    sqlToRelConverter.enableTableAccessConversion(true)

    val rel = sqlToRelConverter.convertQuery(sql, false, true)
    val decorrel = RelDecorrelator.decorrelateQuery(rel)

    log.debug("Relational convert result:\n{}", RelOptUtil.toString(decorrel))
    decorrel
  }

  private def getPrivateField[T](name: String): T = {
    val f = classOf[PlannerImpl].getDeclaredField(name)
    f.setAccessible(true)
    f.get(this).asInstanceOf[T]
  }

  private def invokePrivateMethod[T](name: String): T = {
    val m = classOf[PlannerImpl].getDeclaredMethod(name)
    m.setAccessible(true)
    m.invoke(this).asInstanceOf[T]
  }
}
