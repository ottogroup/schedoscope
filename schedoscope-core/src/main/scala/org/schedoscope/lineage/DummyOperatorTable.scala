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

import java.util

import org.apache.calcite.sql._
import org.apache.calcite.sql.`type`.{ReturnTypes, SqlOperandCountRanges, SqlTypeName}
import org.apache.calcite.sql.parser.SqlParserPos

import scala.collection.JavaConverters._

/**
  * SqlOperatorTable that defines some dummy functions for compatibility with HiveQL.
  *
  * A found dummy function will replace all previously found matches in the operator list.
  *
  * @author Jan Hicken (jhicken)
  */
object DummyOperatorTable extends SqlOperatorTable {

  case class FramingSqlRankFunction(name: String) extends SqlRankFunction(name) {
    override def allowsFraming(): Boolean = true

    override def getOperandCountRange: SqlOperandCountRange = SqlOperandCountRanges.any()
  }

  case class FramingSqlAggFunction(name: String) extends SqlAggFunction(name,
    new SqlIdentifier(name, SqlParserPos.ZERO), SqlKind.OTHER_FUNCTION, ReturnTypes.explicit(SqlTypeName.ANY), null,
    null, SqlFunctionCategory.USER_DEFINED_FUNCTION) {

    override def allowsFraming(): Boolean = true

    override def getOperandCountRange: SqlOperandCountRange = SqlOperandCountRanges.any()

    override def checkOperandTypes(callBinding: SqlCallBinding, throwOnFailure: Boolean): Boolean = true
  }

  private val operatorTable = Seq(
    "CUME_DIST", "DENSE_RANK", "PERCENT_RANK", "RANK", "ROW_NUMBER"
  ).map(n =>
    n -> FramingSqlRankFunction(n)
  ).toMap[String, SqlOperator] ++ Seq(
    "LAG", "LEAD"
  ).map(n =>
    n -> FramingSqlAggFunction(n)
  ) ++ Seq(
    "CONVERT", "TRANSLATE"
  ).map(n =>
    n -> HiveQlFunction(n)
  )

  override def lookupOperatorOverloads(opName: SqlIdentifier, category: SqlFunctionCategory, syntax: SqlSyntax,
                                       operatorList: util.List[SqlOperator]): Unit = {
    val op = operatorTable.get(opName.getSimple.toUpperCase)
    if (op.isDefined) {
      operatorList.clear()
      operatorList.add(op.get)
    }
  }

  override val getOperatorList: util.List[SqlOperator] = operatorTable.values.toList.asJava
}
