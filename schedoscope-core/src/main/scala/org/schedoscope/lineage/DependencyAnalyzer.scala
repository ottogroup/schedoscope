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

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core._
import org.apache.calcite.rex._
import org.apache.calcite.sql.parser.SqlParseException
import org.apache.calcite.tools.{Frameworks, RelConversionException, ValidationException}
import org.schedoscope.dsl.transformations.HiveTransformation
import org.schedoscope.dsl.transformations.Transformation.replaceParameters
import org.schedoscope.dsl.{Field, FieldLike, Parameter, View}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.matching.Regex.Match

/**
  * @author Jan Hicken (jhicken)
  */
object DependencyAnalyzer {

  private val log = LoggerFactory.getLogger(getClass)

  /**
    * A function providing an ordered list of dependency sets for a relation
    */
  type DependencyFunction = (RelNode) => List[FieldSet]
  /**
    * A cache for dependency information grouped by a key
    */
  type DependencyCache = mutable.Map[String, DependencyMap]

  def analyzeDependencies(view: View, recurse: Boolean = false): DependencyMap = {
    analyze(view, recurse, dependenciesOf)
  }

  def analyzeLineage(view: View, recurse: Boolean = false): DependencyMap = {
    analyze(view, recurse, lineageOf)
  }

  private def analyze(view: View, recurse: Boolean, depFunc: DependencyFunction,
                      cache: DependencyCache = mutable.Map()): DependencyMap = {
    cache.put(view.tableName, view.hiveTransformation match {
      case Some(ht) => try {
        getMap(view, ht, depFunc)
      } catch {
        case ex: SqlParseException =>
          log.warn(s"$view's HiveQl cannot be parsed, falling back to blackbox lineage", ex)
          getBlackboxLineage(view)
        case ex: ValidationException =>
          log.warn(s"$view's HiveQl cannot be validated, falling back to blackbox lineage", ex)
          getBlackboxLineage(view)
        case ex: RelConversionException =>
          log.warn(s"$view's HiveQl cannot be converted into a relational expression, falling back to blackbox lineage", ex)
          getBlackboxLineage(view)
      }
      case _ =>
        log.info(s"$view does not use a HiveTransformation, falling back to blackbox lineage")
        getBlackboxLineage(view)
    })

    if (recurse) {
      view.recursiveDependencies.groupBy(_.tableName).foreach {
        case (tblName, views) => cache.put(tblName, analyze(views.head, recurse = false, depFunc, cache))
      }

      cache.put(view.tableName, cache(view.tableName).map[(FieldLike[_], Set[FieldLike[_]]), DependencyMap] {
        case (f, deps) => f -> deps.flatMap {
          case p: Parameter[_] => Some(p)
          case dep: Field[_] => getLeafDependencies(Set(dep), cache)
        }
      })
    }

    cache(view.tableName)
  }

  private def getLeafDependencies(fields: FieldSet, cache: DependencyCache): FieldSet = {
    fields.flatMap { fieldLike =>
      fieldLike match {
        case p: Parameter[_] => Some(p)
        case f: Field[_] =>
          if (f.assignedStructure.get.asInstanceOf[View].dependencies.isEmpty)
            Some(f)
          else {
            val fieldDeps = cache(f.assignedStructure.get.asInstanceOf[View].tableName)(f)
            if (fieldDeps.contains(f)) // ignore dependencies to itself
              Some(f)
            else
              getLeafDependencies(fieldDeps, cache)
          }
      }
    }
  }

  @throws(classOf[SqlParseException])
  private def getMap(view: View, ht: HiveTransformation, depFunc: DependencyFunction): DependencyMap = {
    log.debug("Processing lineage of {}", view)

    val planner = Frameworks.getPlanner(SchedoscopeConfig(view))

    Some(ht.sql).map(
      replaceParameters(_, ht.configuration.toMap)
    ).map(sql =>
      replaceParameters(sql, parseHiveVars(sql))
    ).map { sql =>
      val sqlSelect = sql.substring(sql.indexOfSlice("SELECT"))

      """[^\\];""".r.findFirstMatchIn(sqlSelect) match {
        case Some(m) => sqlSelect.substring(0, m.start + 1)
        case None => sqlSelect
      }
    }.map(
      preprocessSql
    ).map(
      planner.parse
    ).map(
      planner.validate
    ).map(
      planner.convert
    ).map { relNode =>
      log.debug(RelOptUtil.toString(relNode))
      depFunc(relNode)
    }.get.zipWithIndex.map {
      case (set, i) => view.fields(i) -> set
    }.toMap
  }

  /**
    * Provides dependency information for a [[RelNode]].
    * <p>
    * In addition to all dependencies found by [[DependencyAnalyzer.lineageOf()]], the following dependencies are added:
    * <ul>
    *   <li>`JOIN`, `FILTER`: Lineage of the condition's [[RexNode]] is added to all output attributes' lineage</li>
    *   <li>`AGGREGATE`: Lineage of all group set members is added to all aggregated attributes' lineage</li>
    * </ul>
    *
    * @param node the node to analyze
    * @return an ordered list of dependency sets, the nth set belongs to the nth field of the node
    */
  private def dependenciesOf(node: RelNode): List[FieldSet] = node match {
    case ts: TableScan => ts.getTable.unwrap(classOf[SchedoscopeTable]).view.fieldsAndParameters.map(
      f => Set[FieldLike[_]](f)
    ).toList
    case p: Project =>
      val inputDepList = dependenciesOf(p.getInput)
      p.getChildExps.asScala.toList
        .map(lineageIndicesOf)
        .map(_.flatMap(inputDepList).toSet)
    case agg: Aggregate =>
      val inputDepList = dependenciesOf(agg.getInput)
      val groupedColDeps = inputDepList take agg.getGroupSet.cardinality
      val aggColDeps = agg.getAggCallList.asScala
        .map(_.getArgList)
        .map(_.asScala.flatMap(i => inputDepList(i)).toSet)
        .map(_ ++ groupedColDeps.reduce(_ ++ _))
      List() ++ groupedColDeps ++ aggColDeps
    case join: Join =>
      val inputDepList = dependenciesOf(join.getLeft) ++ dependenciesOf(join.getRight)
      val conditionDeps = lineageIndicesOf(join.getCondition).flatMap(inputDepList)
      inputDepList.map(_ ++ conditionDeps)
    case filter: Filter =>
      val inputDepList = dependenciesOf(filter.getInput)
      val conditionDeps = lineageIndicesOf(filter.getCondition).flatMap(inputDepList)
      inputDepList.map(_ ++ conditionDeps)
    case sort: Sort => dependenciesOf(sort.getInput)
    case union: Union => union.getInputs.asScala.map(lineageOf).reduce(
      (l1, l2) => l1.zip(l2).map {
        case (s1, s2) => s1 ++ s2
      }
    )
  }

  /**
    * Provides lineage information for a [[RelNode]].
    * <p>
    * <ul>
    *   <li>`TABLESCAN`: Trivial leaf case, each attribute depends on itself</li>
    *   <li>`PROJECT`: Each output attribute depends on the lineage of all its input attributes</li>
    *   <li>`AGGREGATE`: A grouped attribute depends on itself, an aggregated attribute depends on all operands given to the aggregation function</li>
    *   <li>`JOIN`: Concatenate the left and right hand side's lineage of the join</li>
    *   <li>`FILTER`, `SORT`: Copies the lineage of the input relation</li>
    *   <li>`UNION`: Concatenate the lineage sets of the nth field from each union</li>
    * </ul>
    *
    * @param node the node to analyze
    * @return an ordered list of dependency sets, the nth set belongs to the nth field of the node
    */
  private def lineageOf(node: RelNode): List[FieldSet] = node match {
    case ts: TableScan => ts.getTable.unwrap(classOf[SchedoscopeTable]).view.fieldsAndParameters.map(
      f => Set[FieldLike[_]](f)
    ).toList
    case p: Project =>
      val inputDepList = lineageOf(p.getInput)
      p.getChildExps.asScala.toList
        .map(lineageIndicesOf)
        .map(_.flatMap(inputDepList).toSet)
    case agg: Aggregate =>
      val inputDepList = lineageOf(agg.getInput)
      val groupedColDeps = inputDepList.take(agg.getGroupSet.cardinality)
      val aggColDeps = agg.getAggCallList.asScala
        .map(_.getArgList)
        .map(_.asScala.flatMap(i => inputDepList(i)).toSet)
      List() ++ groupedColDeps ++ aggColDeps
    case semiJoin: SemiJoin => lineageOf(semiJoin.getLeft)
    case join: Join => lineageOf(join.getLeft) ++ lineageOf(join.getRight)
    case filter: Filter => lineageOf(filter.getInput)
    case sort: Sort => lineageOf(sort.getInput)
    case union: Union => union.getInputs.asScala.map(lineageOf).reduce(
      (l1, l2) => l1.zip(l2).map {
        case (s1, s2) => s1 ++ s2
      }
    )
  }

  /**
    * Provides lineage information for a [[RexNode]].
    * <p>
    * The [[RexNode]] is analyzed recursively with its operands, if a [[RexCall]] is found.
    * In case of an [[RexInputRef]], the referenced field index is returned.
    *
    * @param node the node to analyze
    * @return A list of field indices from the input relation the node depends on
    */
  private def lineageIndicesOf(node: RexNode): Set[Int] = node match {
    case inputRef: RexInputRef => Set(inputRef.getIndex)
    case fieldAccess: RexFieldAccess => lineageIndicesOf(fieldAccess.getReferenceExpr)
    case call: RexCall => call.getOperands.asScala.flatMap(lineageIndicesOf).toSet
    case _: RexLiteral => Set()
  }

  /**
    * Provides lineage information for black box transformations.
    * <p>
    * If an explicit lineage for a field has been defined with [[View.affects()]], the information is taken from
    * there. Otherwise, it is assumed, that each field depends on <i>every</i> field from each dependency.
    * <p>
    * If a view does not depend on any views, will return a map where each field depends only on itself.
    *
    * @param view a view
    * @return a map: `f -> Set(all fields from all dependencies)`
    */
  private def getBlackboxLineage(view: View): DependencyMap = {
    if (view.dependencies.isEmpty)
      view.fieldsAndParameters.map(f => f -> Set[FieldLike[_]](f))
    else if (view.explicitLineage.nonEmpty)
      view.fieldsAndParameters.map(f => f -> view.explicitLineage(f).toSet)
    else {
      val allIngoingFields = view.dependencies.flatMap(_.fieldsAndParameters).toSet
      view.fieldsAndParameters.map(f => f -> allIngoingFields)
    }
  }.toMap

  /**
    * Preprocesses a HiveQL statement and produces a Calcite-compatible one.
    *
    * @param sql a HiveQL statement
    * @return a calcite-compatible SQL
    */
  private def preprocessSql(sql: String): String =
    Seq(
      """"[^"\\]*(?:\\.[^"\\]*)*"|'[^'\\]*(?:\\.[^'\\]*)*'""".r -> { (_: Match) =>
        "''"
      }, // replace all double or single quoted strings by empty single quoted strings
      """(?i)([\(\)\.,\s])(year|month|day|hour|minute|second|method)([=\(\)\.,\s])""".r -> { (m: Match) =>
        s"${m.group(1)}`${m.group(2)}`${m.group(3)}"
      }, // escape reserved keywords
      "==".r -> { (_: Match) =>
        "="
      } // replace == with =
    ).foldLeft(sql) {
      case (str, (regex, replacer)) => regex.replaceAllIn(str, replacer)
    }

  private def parseHiveVars(sql: String): Map[String, String] = {
    "(?i)SET hivevar:(.+)=(.+);".r.findAllMatchIn(sql).map(m =>
      m.group(1) -> m.group(2)
    ).toMap
  }
}
