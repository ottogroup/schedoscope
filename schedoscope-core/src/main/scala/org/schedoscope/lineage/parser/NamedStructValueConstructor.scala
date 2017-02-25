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

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.fun.SqlMultisetValueConstructor
import org.apache.calcite.sql.{SqlCallBinding, SqlKind, SqlOperatorBinding}

import scala.collection.JavaConverters._

/**
  * @author Jan Hicken (jhicken)
  */
object NamedStructValueConstructor extends SqlMultisetValueConstructor("NAMED_STRUCT", SqlKind.OTHER_FUNCTION) {

  override def inferReturnType(opBinding: SqlOperatorBinding): RelDataType =
    opBinding.getTypeFactory.createStructType(
      0.until(opBinding.getOperandCount).filter(_ % 2 == 1).map(opBinding.getOperandType).asJava,
      0.until(opBinding.getOperandCount, 2).map(opBinding.getStringLiteralOperand).asJava
    )

  override def checkOperandTypes(callBinding: SqlCallBinding, throwOnFailure: Boolean): Boolean = true
}
