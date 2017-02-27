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

package org.schedoscope.lineage.parser

import java.util

import org.apache.calcite.sql._
import org.apache.calcite.sql.`type`.{ReturnTypes, SqlOperandCountRanges, SqlTypeName}
import org.apache.calcite.sql.parser.SqlParserUtil

/**
  * @author Jan Hicken (jhicken)
  */
object StructFieldAccessOperator extends SqlSpecialOperator("FIELD_ACCESS", SqlKind.OTHER_FUNCTION, 100, true,
  ReturnTypes.explicit(SqlTypeName.ANY), null, null) {

  override def checkOperandTypes(callBinding: SqlCallBinding, throwOnFailure: Boolean): Boolean = true

  override val getOperandCountRange: SqlOperandCountRange = SqlOperandCountRanges.of(2)

  override def reduceExpr(ordinal: Int, list: util.List[AnyRef]): Int = {
    val left = list.get(ordinal - 1).asInstanceOf[SqlNode]
    val item = list.get(ordinal).asInstanceOf[SqlParserUtil.ToTreeListItem]
    val right = list.get(ordinal + 1).asInstanceOf[SqlNode]

    SqlParserUtil.replaceSublist(list, ordinal - 1, ordinal + 2, createCall(
      left.getParserPosition
        .plus(right.getParserPosition)
        .plus(item.getPos),
      left, right))

    ordinal - 1
  }

  override def unparse(writer: SqlWriter, call: SqlCall, leftPrec: Int, rightPrec: Int): Unit = {
    call.operand(0).asInstanceOf[SqlNode].unparse(writer, leftPrec, 0)
    writer.print(".")
    call.operand(1).asInstanceOf[SqlNode].unparse(writer, 0, 0)
  }

}
