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

import com.google.common.collect.ImmutableList
import org.apache.calcite.avatica.util.{Casing, Quoting}
import org.apache.calcite.plan.{Context, RelOptCostFactory, RelTrait, RelTraitDef}
import org.apache.calcite.rel.`type`.RelDataTypeSystem
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.SqlOperatorTable
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.parser.SqlParser.Config
import org.apache.calcite.sql.util.ChainedSqlOperatorTable
import org.apache.calcite.sql2rel.{SqlRexConvertletTable, StandardConvertletTable}
import org.apache.calcite.tools.{FrameworkConfig, Frameworks, Program}
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl
import org.schedoscope.dsl.View
import org.schedoscope.lineage.parser.HiveQlParserImpl

import scala.collection.JavaConverters._

/**
  * @author Jan Hicken (jhicken)
  */
case class SchedoscopeConfig(view: View, scanRecursive: Boolean) extends FrameworkConfig {
  override val getDefaultSchema: SchemaPlus = {
    val rootSchema = Frameworks.createRootSchema(false)
    (if (scanRecursive) view.recursiveDependencies else view.dependencies).groupBy(_.dbName).foreach {
      case (dbName, dbViews) => rootSchema.add(dbName, SchedoscopeSchema(dbViews))
    }

    rootSchema
  }

  override val getCostFactory: RelOptCostFactory = null

  override val getConvertletTable: SqlRexConvertletTable = StandardConvertletTable.INSTANCE

  override val getTraitDefs: ImmutableList[RelTraitDef[_ <: RelTrait]] = ImmutableList.of()

  override val getTypeSystem: RelDataTypeSystem = new HiveTypeSystemImpl

  override val getOperatorTable: SqlOperatorTable = new ChainedSqlOperatorTable(List(
    new SchedoscopeOperatorTable(view),
    HiveQlOperatorTable,
    SqlStdOperatorTable.instance,
    DummyOperatorTable
  ).asJava)

  override val getContext: Context = null

  override val getPrograms: ImmutableList[Program] = ImmutableList.of()

  override val getParserConfig: Config = SqlParser.configBuilder()
    .setParserFactory(HiveQlParserImpl.FACTORY)
    .setCaseSensitive(false)
    .setQuoting(Quoting.BACK_TICK)
    .setQuotedCasing(Casing.UNCHANGED)
    .setUnquotedCasing(Casing.UNCHANGED)
    .build()
}