package org.schedoscope.lineage.parser

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.{SqlCallBinding, SqlOperatorBinding}
import org.apache.calcite.sql.`type`.{SqlTypeName, SqlTypeUtil}
import org.apache.calcite.sql.fun.SqlArrayValueConstructor
import org.apache.calcite.util.Static.RESOURCE

import scala.util.{Failure, Success, Try}

object AllowEmptySqlArrayValueConstructor extends SqlArrayValueConstructor {
  override def checkOperandTypes(callBinding: SqlCallBinding, throwOnFailure: Boolean): Boolean = {
    val argTypes = SqlTypeUtil.deriveAndCollectTypes(callBinding.getValidator, callBinding.getScope,
      callBinding.getCall.getOperandList)

    if (!argTypes.isEmpty
      && getComponentType(callBinding.getTypeFactory, argTypes) == null) {
      if (throwOnFailure) throw callBinding.newValidationError(RESOURCE.needSameTypeParameter)
      else false
    }
    else true
  }

  override def inferReturnType(opBinding: SqlOperatorBinding): RelDataType =
    Try(super.inferReturnType(opBinding)) match {
      case Success(t) => t
      case Failure(_) => SqlTypeUtil.createArrayType(opBinding.getTypeFactory,
        opBinding.getTypeFactory.createSqlType(SqlTypeName.ANY), false)
    }
}
