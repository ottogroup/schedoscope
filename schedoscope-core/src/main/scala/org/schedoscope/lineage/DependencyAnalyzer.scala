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

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core._
import org.apache.calcite.rex._
import org.apache.calcite.tools.ValidationException
import org.schedoscope.dsl.transformations.HiveTransformation
import org.schedoscope.dsl.transformations.Transformation.replaceParameters
import org.schedoscope.dsl.{FieldLike, View}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.matching.Regex.Match
import scala.util.{Failure, Try}

/**
  * @author Jan Hicken (jhicken)
  */
object DependencyAnalyzer {

  private val log = LoggerFactory.getLogger(getClass)

  /**
    * A function providing an ordered list of dependency sets for a relation
    */
  private type DependencyFunction = (RelNode) => List[FieldSet]
  /**
    * A cache for dependency information grouped by a key
    */
  private type DependencyCache = mutable.Map[String, DependencyMap]

  /**
    * Provides ''extended Where-Provenance'' information for a [[org.schedoscope.dsl.View]].
    *
    * ''Extended Where-Provenance'' means, that in addition to the ordinary Where-Provenance dependencies, the
    * following mechanisms are applied:
    * - JOIN, FILTER: the corresponding condition's lineage is added to each field's lineage
    * - AGGREGATE: each field grouped by is added to each field's lineage
    *
    * The view must either
    * - declare a [[org.schedoscope.dsl.transformations.HiveTransformation]]; or
    * - declare the lineage manually with `affects()`
    *
    * If the corresponding HiveQL statement cannot be processed or the view does not declare one of
    * the above, ''blackbox lineage'' is returned instead. This assumes, that each output field is dependent
    * on ''any'' input field.
    *
    * Currently, neither `LATERAL TABLE` nor user-defined table functions (UDTFs) can be processed. Thus,
    * if lineage is not defined explicitly, ''blackbox lineage'' will be returned.
    *
    * @param view a view to analyze
    * @return a map, that assigns a set of dependencies to each field
    */
  def analyzeDependencies(view: View): Try[DependencyMap] = analyze(view, dependenciesOf)

  /**
    * Provides ''Where-Provenance'' information for a [[org.schedoscope.dsl.View]].
    *
    * The view must either
    * - declare a [[org.schedoscope.dsl.transformations.HiveTransformation]]; or
    * - declare the lineage manually with `affects()`
    *
    * If the corresponding HiveQL statement cannot be processed or the view does not declare one of
    * the above, ''blackbox lineage'' is returned instead. This assumes, that each output field is dependent
    * on ''any'' input field.
    *
    * Currently, neither `LATERAL TABLE` nor user-defined table functions (UDTFs) can be processed. Thus,
    * if lineage is not defined explicitly, ''blackbox lineage'' will be returned.
    *
    * @param view a view to analyze
    * @return a map, that assigns a set of dependencies to each field
    */
  def analyzeLineage(view: View): Try[DependencyMap] = analyze(view, lineageOf)

  private def analyze(view: View, depFunc: DependencyFunction): Try[DependencyMap] = {
    view.hiveTransformation match {
      case Some(ht) => Try(getMap(view, ht, depFunc))
      case _ => Failure(new NoHiveTransformationException())
    }
  }

  private def getMap(view: View, ht: HiveTransformation, depFunc: DependencyFunction): DependencyMap = {
    log.debug("Processing lineage of {}", view)

    var planner = new NonFlatteningPlannerImpl(SchedoscopeConfig(view, scanRecursive = false))

    val firstStmt = (Some(ht.sql)
      map (replaceParameters(_, ht.configuration.toMap))
      map { sql => replaceParameters(sql, parseHiveVars(sql)) }
      map preprocessSql
      map { sql =>
      """(?!\\);""".r.findFirstMatchIn(sql) match {
        case Some(m) => sql.substring(0, m.start)
        case None => sql
      }
    }
      ).get

    val parsed = planner.parse(firstStmt)

    // try with direct dependencies first, then recursively
    val validated = try {
      planner.validate(parsed)
    } catch {
      case _: ValidationException =>
        log.debug("Trying again with a recursively-built schema...")
        planner = new NonFlatteningPlannerImpl(SchedoscopeConfig(view, scanRecursive = true))
        planner.validate(planner.parse(firstStmt))
    }

    val relNode = planner.convert(validated)
    depFunc(relNode).zipWithIndex.map {
      case (set, i) => view.fields(i) -> set
    }.toMap
  }

  /**
    * Provides dependency information for a [[org.apache.calcite.rel.RelNode]].
    * <p>
    * In addition to all dependencies found by `lineageOf()`, the following dependencies are added:
    * <ul>
    * <li>`JOIN`, `FILTER`: Lineage of the condition's [[RexNode]] is added to all output attributes' lineage</li>
    * <li>`AGGREGATE`: Lineage of all group set members is added to all aggregated attributes' lineage</li>
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
        .map(_ ++ groupedColDeps.flatten)
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
    * Provides lineage information for a [[org.apache.calcite.rel.RelNode]].
    * <p>
    * <ul>
    * <li>`TABLESCAN`: Trivial leaf case, each attribute depends on itself</li>
    * <li>`PROJECT`: Each output attribute depends on the lineage of all its input attributes</li>
    * <li>`AGGREGATE`: A grouped attribute depends on itself, an aggregated attribute depends on all operands given to the aggregation function</li>
    * <li>`JOIN`: Concatenate the left and right hand side's lineage of the join</li>
    * <li>`FILTER`, `SORT`: Copies the lineage of the input relation</li>
    * <li>`UNION`: Concatenate the lineage sets of the nth field from each union</li>
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
    * Provides lineage information for a [[org.apache.calcite.rex.RexNode]].
    * <p>
    * The [[org.apache.calcite.rex.RexNode]] is analyzed recursively with its operands, if a
    * [[org.apache.calcite.rex.RexCall]] is found. In case of an [[org.apache.calcite.rex.RexInputRef]],
    * the referenced field index is returned.
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
    * If an explicit lineage for a field has been defined with `View.affects()`, the information is taken from
    * there. Otherwise, it is assumed, that each field depends on <i>every</i> field from each dependency.
    * <p>
    * If a view does not depend on any views, will return a map where each field depends only on itself.
    *
    * @param view a view
    * @return a map: `f -> Set(all fields from all dependencies)`
    */
  def getBlackboxLineage(view: View): DependencyMap = {
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
      """--.*?\n""".r -> { (_: Match) =>
        ""
      }, // remove all comments
      """"[^"\\]*(?:\\.[^"\\]*)*"|'[^'\\]*(?:\\.[^'\\]*)*'""".r -> { (_: Match) =>
        "''"
      }, // replace all double or single quoted strings by empty single quoted strings
      """(?i)SET .+?=.+?;""".r -> { (_: Match) =>
        ""
      } // remove all SET option=value; expressions
    ).foldLeft(sql) {
      case (str, (regex, replacer)) => regex.replaceAllIn(str, replacer)
    }

  private def parseHiveVars(sql: String): Map[String, String] = {
    "(?i)SET hivevar:(.+)=(.+);".r.findAllMatchIn(sql).map(m =>
      m.group(1) -> m.group(2)
    ).toMap
  }
}
