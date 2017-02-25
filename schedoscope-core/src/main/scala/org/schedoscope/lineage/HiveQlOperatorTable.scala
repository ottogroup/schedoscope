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

import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.{SqlFunctionCategory, SqlIdentifier, SqlOperator, SqlOperatorTable, SqlSyntax}
import org.apache.hadoop.hive.ql.exec.FunctionRegistry
import org.schedoscope.lineage.parser.{NamedStructValueConstructor, StructFieldAccessOperator, StructValueConstructor}

import scala.collection.JavaConverters._

/**
  * SqlOperatorTable for Hive's [[org.apache.hadoop.hive.ql.exec.FunctionRegistry]].
  *
  * @author Jan Hicken (jhicken)
  */
object HiveQlOperatorTable extends SqlOperatorTable {
  val STRUCT_FIELD_ACCESS = StructFieldAccessOperator
  val STRUCT_VALUE_CONSTRUCTOR = StructValueConstructor
  val NAMED_STRUCT_VALUE_CONSTRUCTOR = NamedStructValueConstructor

  private val knownReturnTypes = Map(
    "array_contains" -> SqlTypeName.BOOLEAN
  )

  override def lookupOperatorOverloads(opName: SqlIdentifier, category: SqlFunctionCategory, syntax: SqlSyntax,
                                       operatorList: util.List[SqlOperator]): Unit =
    FunctionRegistry.getFunctionNames("(?i)" + opName.getSimple).asScala.map(
      FunctionRegistry.getFunctionInfo
    ).flatMap { fi =>
      if (fi.isGenericUDF || fi.isGenericUDTF) Some(
        HiveQlFunction(fi.getDisplayName,
          knownReturnTypes.getOrElse(fi.getDisplayName.toLowerCase, SqlTypeName.ANY)))
      else if (fi.isGenericUDAF) Some(HiveQlAggFunction(fi.getDisplayName))
      else None
    }.foreach(
      operatorList.add
    )

  override lazy val getOperatorList: util.List[SqlOperator] = {
    List[SqlOperator]() ++ FunctionRegistry.getFunctionNames().asScala.map(
      FunctionRegistry.getFunctionInfo
    ).flatMap { fi =>
      if (fi.isGenericUDF || fi.isGenericUDTF) Some(
        HiveQlFunction(fi.getDisplayName,
          knownReturnTypes.getOrElse(fi.getDisplayName.toLowerCase, SqlTypeName.ANY)))
      else if (fi.isGenericUDAF) Some(HiveQlAggFunction(fi.getDisplayName))
      else None
    }
  }.asJava
}
