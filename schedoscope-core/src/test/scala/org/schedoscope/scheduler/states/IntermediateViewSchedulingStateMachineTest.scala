/**
 * Copyright 2015 Otto (GmbH & Co KG)
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
package org.schedoscope.scheduler.states

import org.scalatest.{FlatSpec, Matchers}
import org.schedoscope.dsl.Parameter.p
import org.schedoscope.scheduler.messages.MaterializeViewMode._
import org.schedoscope.scheduler.states.PartyInterestedInViewSchedulingStateChange._
import test.eci.datahub.{ProductBrandMaterializeOnce, ProductBrandsNoOpMirror}

class IntermediateViewSchedulingStateMachineTest extends FlatSpec with Matchers {

  trait IntermediateView {
    val stateMachine = new ViewSchedulingStateMachineImpl()
    val dependentView = ProductBrandsNoOpMirror(p("2014"), p("01"), p("01"))
    val anotherDependentView = ProductBrandsNoOpMirror(p("2014"), p("01"), p("02"))
    val viewUnderTest = dependentView.dependencies.head
    val firstDependency = viewUnderTest.dependencies.head
    val secondDependency = viewUnderTest.dependencies(1)
    val materializeOnceView = ProductBrandMaterializeOnce(p("EC0101"), p("2014"), p("01"), p("02"))
    val viewTransformationChecksum = viewUnderTest.transformation().checksum
  }

  "An intermediate view in CreatedByViewManager state" should "transition to Waiting upon materialize" in new IntermediateView {
    val startState = CreatedByViewManager(viewUnderTest)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          `viewUnderTest`, org.schedoscope.dsl.transformations.Checksum.defaultDigest, 0,
          dependenciesMaterializing, interestedParties,
          DEFAULT, false, false, false, 0), _) =>
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        interestedParties should contain(DependentView(dependentView))
      case _ => fail()
    }
  }

  it should "ask its dependencies to materialize" in new IntermediateView {
    val startState = CreatedByViewManager(viewUnderTest)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) =>
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest, DEFAULT))
        }
      case _ => fail()
    }
  }

  it should "remember the materialization mode" in new IntermediateView {
    val startState = CreatedByViewManager(viewUnderTest)

    stateMachine.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          _, _, _, _, _,
          materializationMode,
          _, _, _, _), _) =>
        materializationMode shouldBe RESET_TRANSFORMATION_CHECKSUMS
      case _ => fail()
    }
  }

  it should "pass the materialization mode to the dependencies" in new IntermediateView {
    val startState = CreatedByViewManager(viewUnderTest)

    stateMachine.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) =>
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest, RESET_TRANSFORMATION_CHECKSUMS))
        }
      case _ => fail()
    }
  }

  it should "transition to Invalidated upon invalidate" in new IntermediateView {
    val startState = CreatedByViewManager(viewUnderTest)

    stateMachine.invalidate(startState, dependentView) match {
      case ResultingViewSchedulingState(Invalidated(view), s) =>
        view shouldBe viewUnderTest
        s shouldEqual Set(ReportInvalidated(view, Set(dependentView)))
    }
  }

  "An intermediate view in Invalidated state" should "transition to Waiting upon materialize" in new IntermediateView {
    val startState = Invalidated(viewUnderTest)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          `viewUnderTest`, org.schedoscope.dsl.transformations.Checksum.defaultDigest, 0,
          dependenciesMaterializing, interestedParties,
          DEFAULT, false, false, false, 0), _) =>
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        interestedParties should contain(DependentView(dependentView))
      case _ => fail()
    }
  }

  it should "ask its dependencies to materialize" in new IntermediateView {
    val startState = Invalidated(viewUnderTest)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) =>
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest, DEFAULT))
        }
      case _ => fail()
    }
  }

  it should "remember the materialization mode" in new IntermediateView {
    val startState = Invalidated(viewUnderTest)

    stateMachine.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          _, _, _, _, _,
          materializationMode,
          _, _, _, _), _) =>
        materializationMode shouldBe RESET_TRANSFORMATION_CHECKSUMS
      case _ => fail()
    }
  }

  it should "pass the materialization mode to the dependencies" in new IntermediateView {
    val startState = Invalidated(viewUnderTest)

    stateMachine.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) =>
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest, RESET_TRANSFORMATION_CHECKSUMS))
        }
      case _ => fail()
    }
  }

  it should "stay in Invalidated upon invalidate" in new IntermediateView {
    val startState = Invalidated(viewUnderTest)

    stateMachine.invalidate(startState, dependentView) match {
      case ResultingViewSchedulingState(Invalidated(view), s) =>
        view shouldBe viewUnderTest
        s shouldEqual Set(ReportInvalidated(view, Set(dependentView)))
    }
  }

  "An intermediate view in NoData state" should "transition to Waiting upon materialize" in new IntermediateView {
    val startState = NoData(viewUnderTest)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          `viewUnderTest`, org.schedoscope.dsl.transformations.Checksum.defaultDigest, 0,
          dependenciesMaterializing, interestedParties,
          DEFAULT, false, false, false, 0), _) =>
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        interestedParties should contain(DependentView(dependentView))
      case _ => fail()
    }
  }

  it should "ask its dependencies to materialize" in new IntermediateView {
    val startState = NoData(viewUnderTest)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) =>
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest, DEFAULT))
        }
      case _ => fail()
    }
  }

  it should "transition to Invalidated upon invalidate" in new IntermediateView {
    val startState = NoData(viewUnderTest)

    stateMachine.invalidate(startState, dependentView) match {
      case ResultingViewSchedulingState(Invalidated(view), s) =>
        view shouldBe viewUnderTest
        s shouldEqual Set(ReportInvalidated(view, Set(dependentView)))
    }
  }

  "An intermediate view in ReadFromSchemaManager state" should "transition to Waiting upon materialize" in new IntermediateView {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    stateMachine.materialize(startState, dependentView, currentTime = 20) match {
      case ResultingViewSchedulingState(
        Waiting(
          `viewUnderTest`, `viewTransformationChecksum`, 10,
          dependenciesMaterializing, interestedParties,
          DEFAULT, false, false, false, 0), _) =>
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        interestedParties should contain(DependentView(dependentView))
      case _ => fail()
    }
  }

  it should "ask its dependencies to materialize" in new IntermediateView {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) =>
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest, DEFAULT))
        }
      case _ => fail()
    }
  }

  it should "remember the materialization mode" in new IntermediateView {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    stateMachine.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          _, _, _, _, _,
          materializationMode,
          _, _, _, _), _) =>
        materializationMode shouldBe RESET_TRANSFORMATION_CHECKSUMS
      case _ => fail()
    }
  }

  it should "pass the materialization mode to the dependencies" in new IntermediateView {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    stateMachine.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) =>
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest, RESET_TRANSFORMATION_CHECKSUMS))
        }
      case _ => fail()
    }
  }

  it should "transition to Invalidated upon invalidate" in new IntermediateView {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    stateMachine.invalidate(startState, dependentView) match {
      case ResultingViewSchedulingState(Invalidated(view), s) =>
        view shouldBe viewUnderTest
        s shouldEqual Set(ReportInvalidated(view, Set(dependentView)))
    }
  }

  it should "become Materialized upon materialize if materialize once is set" in new IntermediateView {
    val startState = ReadFromSchemaManager(materializeOnceView, viewTransformationChecksum, 10)

    stateMachine.materialize(startState, dependentView, currentTime = 20) match {
      case ResultingViewSchedulingState(
        Materialized(
          view,
          `viewTransformationChecksum`,
          10,
          false,
          false), s) =>
        view shouldBe materializeOnceView
        s shouldEqual Set(
          ReportMaterialized(
            materializeOnceView,
            Set(dependentView),
            10,
            withErrors = false,
            incomplete = false))

      case _ => fail()
    }
  }

  it should "become Materialized upon materialize if materialize once is set and write transformation checksum if RESET_TRANSFORMATION_CHECKSUMS is set" in new IntermediateView {
    val startState = ReadFromSchemaManager(materializeOnceView, viewTransformationChecksum, 10)

    stateMachine.materialize(startState, dependentView, RESET_TRANSFORMATION_CHECKSUMS, currentTime = 20) match {
      case ResultingViewSchedulingState(
        Materialized(_, _, _, _, _), s) =>
        s should contain(WriteTransformationCheckum(materializeOnceView))

      case _ => fail()
    }
  }

  "An intermediate view in Materialized state" should "transition to Waiting upon materialize" in new IntermediateView {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10, withErrors = false, incomplete = false)

    stateMachine.materialize(startState, dependentView, currentTime = 20) match {
      case ResultingViewSchedulingState(
        Waiting(
          `viewUnderTest`, `viewTransformationChecksum`, 10,
          dependenciesMaterializing, interestedParties,
          DEFAULT, false, false, false, 0), _) =>
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        interestedParties should contain(DependentView(dependentView))
      case _ => fail()
    }
  }

  it should "ask its dependencies to materialize" in new IntermediateView {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10, withErrors = false, incomplete = false)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) =>
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest, DEFAULT))
        }
      case _ => fail()
    }
  }

  it should "remember the materialization mode" in new IntermediateView {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10, withErrors = false, incomplete = false)

    stateMachine.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          _, _, _, _, _,
          materializationMode,
          _, _, _, _), _) =>
        materializationMode shouldBe RESET_TRANSFORMATION_CHECKSUMS
      case _ => fail()
    }
  }

  it should "pass the materialization mode to the dependencies" in new IntermediateView {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10, withErrors = false, incomplete = false)

    stateMachine.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) =>
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest, RESET_TRANSFORMATION_CHECKSUMS))
        }
      case _ => fail()
    }
  }

  it should "transition to Invalidated upon invalidate" in new IntermediateView {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10, withErrors = false, incomplete = false)

    stateMachine.invalidate(startState, dependentView) match {
      case ResultingViewSchedulingState(Invalidated(view), s) =>
        view shouldBe viewUnderTest
        s shouldEqual Set(ReportInvalidated(view, Set(dependentView)))
    }
  }

  it should "stay Materialized upon materialize if materialize once is set" in new IntermediateView {
    val startState = Materialized(materializeOnceView, viewTransformationChecksum, 10, withErrors = false, incomplete = false)

    stateMachine.materialize(startState, dependentView, currentTime = 20) match {
      case ResultingViewSchedulingState(
        Materialized(
          view,
          `viewTransformationChecksum`,
          10,
          false,
          false), s) =>
        view shouldBe materializeOnceView
        s shouldEqual Set(
          ReportMaterialized(
            materializeOnceView,
            Set(dependentView),
            10,
            withErrors = false,
            incomplete = false))

      case _ => fail()
    }
  }

  it should "become Materialized upon materialize if materialize once is set and write transformation checksum if RESET_TRANSFORMATION_CHECKSUMS is set" in new IntermediateView {
    val startState = Materialized(materializeOnceView, viewTransformationChecksum, 10, withErrors = false, incomplete = false)

    stateMachine.materialize(startState, dependentView, RESET_TRANSFORMATION_CHECKSUMS, currentTime = 20) match {
      case ResultingViewSchedulingState(
        Materialized(_, _, _, _, _), s) =>
        s should contain(WriteTransformationCheckum(materializeOnceView))

      case _ => fail()
    }
  }

  "A NoOp intermediate view in Waiting state" should "stay in Waiting adding another listener upon materialize" in new IntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      viewUnderTest.dependencies.toSet, Set(anotherDependentView),
      DEFAULT, oneDependencyReturnedData = false, withErrors = false, incomplete = false, 0)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          `viewUnderTest`, `viewTransformationChecksum`, 10,
          dependenciesMaterializing, interestedParties,
          DEFAULT, false, false, false, 0), s) =>
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        interestedParties should contain(DependentView(dependentView))
        interestedParties should contain(DependentView(anotherDependentView))
        s shouldBe 'empty
      case _ => fail()
    }
  }

  it should "stay waiting and report NotInvalidated upon invalidate" in new IntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      viewUnderTest.dependencies.toSet, Set(anotherDependentView),
      DEFAULT, oneDependencyReturnedData = false, withErrors = false, incomplete = false, 0)

    stateMachine.invalidate(startState, dependentView) match {

      case ResultingViewSchedulingState(resultState, s) =>
        resultState shouldEqual startState
        s shouldEqual Set(ReportNotInvalidated(viewUnderTest, Set(dependentView)))
      case _ => fail()
    }
  }

  "An intermediate view in Waiting state receiving materialized" should "stay in Waiting if not all dependencies have answered yet" in new IntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      viewUnderTest.dependencies.toSet, Set(dependentView),
      DEFAULT, oneDependencyReturnedData = false, withErrors = false, incomplete = false, 0)

    stateMachine.materialized(startState, firstDependency, 15, 20) match {
      case ResultingViewSchedulingState(
        Waiting(
          view,
          `viewTransformationChecksum`,
          10,
          waitingForDependencies, dependViews,
          DEFAULT,
          true, false, false, 15
          ), _) =>
        waitingForDependencies shouldEqual Set(secondDependency)
        dependViews shouldEqual Set(DependentView(dependentView))
        view shouldBe viewUnderTest

      case _ => fail()
    }
  }

  it should "leave incomplete / error settings untouched when remaining in waiting" in new IntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      viewUnderTest.dependencies.toSet, Set(dependentView),
      DEFAULT, oneDependencyReturnedData = false, withErrors = true, incomplete = true, 0)

    stateMachine.materialized(startState, firstDependency, 15, 20) match {
      case ResultingViewSchedulingState(
        Waiting(
          view,
          `viewTransformationChecksum`,
          10,
          waitingForDependencies, dependViews,
          DEFAULT,
          true, true, true, 15
          ), _) =>
        waitingForDependencies shouldEqual Set(secondDependency)
        dependViews shouldEqual Set(DependentView(dependentView))
        view shouldBe viewUnderTest

      case _ => fail()
    }
  }

  it should "transition to and report NoDataAvailable if no dependencies answered with Materialized" in new IntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = false, withErrors = false, incomplete = false, 0)

    stateMachine.noDataAvailable(startState, firstDependency, 20) match {
      case ResultingViewSchedulingState(
        NoData(view),
        s) =>
        view shouldBe viewUnderTest
        s shouldEqual Set(ReportNoDataAvailable(viewUnderTest, Set(dependentView)))

      case _ => fail()
    }
  }

  it should "transition to Transforming and demand transformation if the last dependency answers with materialized and is newer" in new IntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = false, withErrors = false, incomplete = false, 0)

    stateMachine.materialized(startState, firstDependency, 15, 20) match {
      case ResultingViewSchedulingState(
        Transforming(
          view,
          lastTransformationChecksum,
          interestedParties,
          DEFAULT,
          false,
          false,
          0
          ),
        s) =>
        interestedParties shouldEqual Set(DependentView(dependentView))
        view shouldBe viewUnderTest
        s shouldEqual Set(Transform(viewUnderTest))

      case _ => fail()
    }
  }

  it should "transition to Transforming and demand transformation if at least one dependency has answered with materialized and it is older" in new IntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 15)

    stateMachine.noDataAvailable(startState, firstDependency, 20) match {
      case ResultingViewSchedulingState(
        Transforming(
          view,
          lastTransformationChecksum,
          interestedParties,
          DEFAULT,
          false,
          false,
          0
          ),
        s) =>
        interestedParties shouldEqual Set(DependentView(dependentView))
        view shouldBe viewUnderTest
        s shouldEqual Set(Transform(viewUnderTest))

      case _ => fail()
    }
  }

  it should "skip Transforming and report Materialize when in mode RESET_TRANSFORMATION_CHECKSUMS_AND_TIMESTAMPS even if at least one dependency has answered with materialized and it is older" in new IntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      Set(firstDependency), Set(dependentView),
      RESET_TRANSFORMATION_CHECKSUMS_AND_TIMESTAMPS, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 15)

    stateMachine.noDataAvailable(startState, firstDependency, 20) match {
      case ResultingViewSchedulingState(
        Materialized(
          view,
          `viewTransformationChecksum`,
          20,
          false,
          true), s) =>
        view shouldBe viewUnderTest
        s should contain(
          ReportMaterialized(
            viewUnderTest,
            Set(dependentView),
            20,
            withErrors = false,
            incomplete = true))

      case _ => fail()
    }
  }

  it should "skip Transforming and write transformation checksum when in mode RESET_TRANSFORMATION_CHECKSUMS_AND_TIMESTAMPS and the checksum has changed even if at least one dependency has answered with materialized and it is older" in new IntermediateView {
    val startState = Waiting(
      viewUnderTest, "outdated checksum", 10,
      Set(firstDependency), Set(dependentView),
      RESET_TRANSFORMATION_CHECKSUMS_AND_TIMESTAMPS, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 15)

    stateMachine.noDataAvailable(startState, firstDependency, 20) match {
      case ResultingViewSchedulingState(
        Materialized(_, _, _, _, _), s) =>
        s should contain(
          WriteTransformationCheckum(viewUnderTest))

      case _ => fail()
    }
  }

  it should "skip Transforming and not write transformation checksum when in mode RESET_TRANSFORMATION_CHECKSUMS_AND_TIMESTAMPS and the checksum has not changed even if at least one dependency has answered with materialized and it is older" in new IntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      Set(firstDependency), Set(dependentView),
      RESET_TRANSFORMATION_CHECKSUMS_AND_TIMESTAMPS, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 15)

    stateMachine.noDataAvailable(startState, firstDependency, 20) match {
      case ResultingViewSchedulingState(
        Materialized(_, _, _, _, _), s) =>
        s should not(contain(
          WriteTransformationCheckum(viewUnderTest)))

      case _ => fail()
    }
  }

  it should "skip Transforming and write transformation timestamp when in mode RESET_TRANSFORMATION_CHECKSUMS_AND_TIMESTAMPS even if at least one dependency has answered with materialized and it is older" in new IntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      Set(firstDependency), Set(dependentView),
      RESET_TRANSFORMATION_CHECKSUMS_AND_TIMESTAMPS, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 15)

    stateMachine.noDataAvailable(startState, firstDependency, 20) match {
      case ResultingViewSchedulingState(
        Materialized(_, _, _, _, _), s) =>
        s should contain(
          WriteTransformationTimestamp(viewUnderTest, 20))

      case _ => fail()
    }
  }

  it should "transition to Materialized if at least one dependency has answered with materialized and it is newer" in new IntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 5)

    stateMachine.materialized(startState, firstDependency, 5, 20) match {
      case ResultingViewSchedulingState(
        Materialized(
          view,
          `viewTransformationChecksum`,
          10,
          false,
          false), s) =>
        view shouldBe viewUnderTest
        s shouldEqual Set(
          ReportMaterialized(
            viewUnderTest,
            Set(dependentView),
            10,
            withErrors = false,
            incomplete = false))

      case _ => fail()
    }
  }

  it should "transition to Materialized and record transformation checksum if at least one dependency has answered with materialized and it is newer and RESET_TRANSFORMATION_CHECKSUMS is set" in new IntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      Set(firstDependency), Set(dependentView),
      RESET_TRANSFORMATION_CHECKSUMS, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 5)

    stateMachine.materialized(startState, firstDependency, 5, 20) match {
      case ResultingViewSchedulingState(
        Materialized(_, _, _, _, _), s) =>
        s should contain(WriteTransformationCheckum(viewUnderTest))

      case _ => fail()
    }
  }

  "An intermediate view in Transforming state" should "transition to Materialized when getting a transformation succeeded notification and the folder is not empty" in new IntermediateView {
    val startState = Transforming(
      viewUnderTest, viewTransformationChecksum,
      Set(dependentView),
      DEFAULT, withErrors = false, incomplete = false, 0)

    stateMachine.transformationSucceeded(startState, folderEmpty = false, 20) match {
      case ResultingViewSchedulingState(
        Materialized(
          view,
          `viewTransformationChecksum`,
          20,
          false,
          false), s) =>
        view shouldBe viewUnderTest
        s should contain(
          ReportMaterialized(
            viewUnderTest,
            Set(dependentView),
            20,
            withErrors = false,
            incomplete = false))

      case _ => fail()
    }
  }

  it should "write a new transformation timestamp when the folder is not empty" in new IntermediateView {
    val startState = Transforming(
      viewUnderTest, viewTransformationChecksum,
      Set(dependentView),
      DEFAULT, withErrors = false, incomplete = false, 0)

    stateMachine.transformationSucceeded(startState, folderEmpty = false, 20) match {
      case ResultingViewSchedulingState(_, s) =>
        s should contain(
          WriteTransformationTimestamp(viewUnderTest, 20))

      case _ => fail()
    }
  }

  it should "touch a _SUCCESS flag when the folder is not empty" in new IntermediateView {
    val startState = Transforming(
      viewUnderTest, viewTransformationChecksum,
      Set(dependentView),
      DEFAULT, withErrors = false, incomplete = false, 0)

    stateMachine.transformationSucceeded(startState, folderEmpty = false, 20) match {
      case ResultingViewSchedulingState(_, s) =>
        s should contain(
          TouchSuccessFlag(viewUnderTest))

      case _ => fail()
    }
  }
  
  it should "write a new transformation checksum if it changed and the folder is not empty" in new IntermediateView {
    val startState = Transforming(
      viewUnderTest, "outdated checksum",
      Set(dependentView),
      DEFAULT, withErrors = false, incomplete = false, 0)

    stateMachine.transformationSucceeded(startState, folderEmpty = false, 20) match {
      case ResultingViewSchedulingState(_, s) =>
        s should contain(
          WriteTransformationCheckum(viewUnderTest))

      case _ => fail()
    }
  }

  it should "not write a new transformation checksum if it changed and the folder is not empty" in new IntermediateView {
    val startState = Transforming(
      viewUnderTest, viewTransformationChecksum,
      Set(dependentView),
      DEFAULT, withErrors = false, incomplete = false, 0)

    stateMachine.transformationSucceeded(startState, folderEmpty = false, 20) match {
      case ResultingViewSchedulingState(_, s) =>
        s should not(contain(
          WriteTransformationCheckum(viewUnderTest)))

      case _ => fail()
    }
  }

  it should "propagate error & completeness information upon materialization when the folder is not empty" in new IntermediateView {
    val startState = Transforming(
      viewUnderTest, viewTransformationChecksum,
      Set(dependentView),
      DEFAULT, withErrors = true, incomplete = true, 0)

    stateMachine.transformationSucceeded(startState, folderEmpty = false, 20) match {
      case ResultingViewSchedulingState(
        Materialized(
          view,
          `viewTransformationChecksum`,
          20,
          true,
          true), s) =>
        view shouldBe viewUnderTest
        s should contain(
          ReportMaterialized(
            viewUnderTest,
            Set(dependentView),
            20,
            withErrors = true,
            incomplete = true))

      case _ => fail()
    }
  }

  it should "enter Retrying upon failure if retries have not been exhausted" in new IntermediateView {
    val startState = Transforming(
      viewUnderTest, viewTransformationChecksum,
      Set(dependentView),
      DEFAULT, withErrors = false, incomplete = false, 1)

    stateMachine.transformationFailed(startState, 2, 20) match {
      case ResultingViewSchedulingState(
        Retrying(
          view, `viewTransformationChecksum`,
          interestedParties,
          DEFAULT, false, false, 2), s) =>
        view shouldBe viewUnderTest
        interestedParties shouldEqual Set(DependentView(dependentView))
        s shouldBe 'empty

      case _ => fail()
    }
  }

  it should "enter and report Failed upon failure if retries have been exhausted" in new IntermediateView {
    val startState = Transforming(
      viewUnderTest, viewTransformationChecksum,
      Set(dependentView),
      DEFAULT, withErrors = false, incomplete = false, 2)

    stateMachine.transformationFailed(startState, 2, 20) match {
      case ResultingViewSchedulingState(
        Failed(view),
        s) =>
        view shouldBe viewUnderTest
        s shouldEqual Set(ReportFailed(viewUnderTest, Set(DependentView(dependentView))))

      case _ => fail()
    }
  }

  it should "transition to and report NoData when getting a transformation succeeded notification but the folder is empty" in new IntermediateView {
    val startState = Transforming(
      viewUnderTest, viewTransformationChecksum,
      Set(dependentView),
      DEFAULT, withErrors = false, incomplete = false, 0)

    stateMachine.transformationSucceeded(startState, folderEmpty = true, 20) match {
      case ResultingViewSchedulingState(NoData(view), s) =>
        view shouldBe viewUnderTest
        s shouldEqual Set(ReportNoDataAvailable(viewUnderTest, Set(DependentView(dependentView))))

      case _ => fail()
    }
  }

  "An intermediate view in Retrying state" should "transition to Transforming upon materialize" in new IntermediateView {
    val startState = Retrying(
      viewUnderTest, viewTransformationChecksum,
      Set(dependentView),
      DEFAULT, withErrors = false, incomplete = false, 2)

    stateMachine.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Transforming(
          view, `viewTransformationChecksum`,
          interestedParties,
          DEFAULT, false, false, 2),
        s) =>
        view shouldBe viewUnderTest
        interestedParties shouldEqual Set(DependentView(dependentView))
        s shouldBe 'empty

      case _ => fail()
    }
  }

  it should "transition to Transforming while preserving error and incompleteness information upon materialize" in new IntermediateView {
    val startState = Retrying(
      viewUnderTest, viewTransformationChecksum,
      Set(dependentView),
      DEFAULT, withErrors = true, incomplete = true, 2)

    stateMachine.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Transforming(
          view,
          `viewTransformationChecksum`,
          interestedParties,
          DEFAULT, true, true, 2),
        s) =>
        view shouldBe viewUnderTest
        interestedParties shouldEqual Set(DependentView(dependentView))
        s shouldBe 'empty

      case _ => fail()
    }
  }

}