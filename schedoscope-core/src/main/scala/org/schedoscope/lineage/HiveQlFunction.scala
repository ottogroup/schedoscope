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

import org.apache.calcite.sql._
import org.apache.calcite.sql.`type`._
import org.apache.calcite.sql.parser.SqlParserPos

/**
  * @author Jan Hicken (jhicken)
  */
case class HiveQlFunction(name: String, returnType: SqlTypeName = SqlTypeName.ANY) extends SqlFunction(
  new SqlIdentifier(name, SqlParserPos.ZERO), ReturnTypes.explicit(returnType), InferTypes.RETURN_TYPE,
  OperandTypes.ANY, null, SqlFunctionCategory.USER_DEFINED_FUNCTION) {

  override def getOperandCountRange: SqlOperandCountRange = SqlOperandCountRanges.any()

  override def checkOperandTypes(callBinding: SqlCallBinding, throwOnFailure: Boolean): Boolean = true
}
