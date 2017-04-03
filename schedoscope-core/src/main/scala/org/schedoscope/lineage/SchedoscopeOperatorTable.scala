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
import org.apache.hadoop.hive.ql.udf.generic.{GenericUDAFResolver, GenericUDF, GenericUDTF}
import org.schedoscope.dsl.View

import scala.collection.JavaConverters._

/**
  * SqlOperatorTable for registered [[org.apache.hadoop.hive.metastore.api.Function]]s in Schedoscope [[org.schedoscope.dsl.transformations.HiveTransformation]]s.
  *
  * @author Jan Hicken (jhicken)
  */
class SchedoscopeOperatorTable(views: Traversable[View]) extends SqlOperatorTable {
  private val udfs = views.groupBy(_.getClass).flatMap(_._2.head.hiveTransformation).flatMap(_.udfs)

  override def lookupOperatorOverloads(opName: SqlIdentifier, category: SqlFunctionCategory, syntax: SqlSyntax,
                                       operatorList: util.List[SqlOperator]): Unit = {
    udfs.filter( f =>
      if (opName.isSimple)
        f.getFunctionName.equalsIgnoreCase(opName.getSimple)
      else
        f.getDbName.equalsIgnoreCase(opName.getComponent(0).getSimple) &&
        f.getFunctionName.equalsIgnoreCase(opName.getComponent(1).getSimple)
    ).map(
      func => func.getFunctionName -> Class.forName(func.getClassName).newInstance()
    ).flatMap {
      case (name, _: GenericUDF) => Some(HiveQlFunction(name))
      case (name, _: GenericUDTF) => Some(HiveQlFunction(name))
      case (name, _: GenericUDAFResolver) => Some(HiveQlAggFunction(name))
      case _ => None
    }.foreach(
      operatorList.add
    )
  }

  override lazy val getOperatorList: util.List[SqlOperator] = udfs.map(
    func => func.getFunctionName -> Class.forName(func.getClassName).newInstance()
  ).flatMap {
    case (name, _: GenericUDF) => Some(HiveQlFunction(name).asInstanceOf[SqlOperator])
    case (name, _: GenericUDTF) => Some(HiveQlFunction(name).asInstanceOf[SqlOperator])
    case (name, _: GenericUDAFResolver) => Some(HiveQlAggFunction(name).asInstanceOf[SqlOperator])
    case _ => None
  }.toList.asJava
}