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
import test.eci.datahub.{ProductBrandsNoOpMirror, ProductBrandsNoOpMirrorDependent}

class NoOpIntermediateViewSchedulingStateMachineTest extends FlatSpec with Matchers {

  trait SuccessFlag {
    val stateMachine = new NoOpViewSchedulingStateMachineImpl(() => true)
  }

  trait NoSuccessFlag {
    val stateMachine = new NoOpViewSchedulingStateMachineImpl(() => false)
  }

  trait NoOpIntermediateView {
    val viewUnderTest = ProductBrandsNoOpMirror(p("2014"), p("01"), p("01"))
    val firstDependency = viewUnderTest.dependencies.head
    val secondDependency = viewUnderTest.dependencies(1)
    val viewTransformationChecksum = viewUnderTest.transformation().checksum
    val dependentView = ProductBrandsNoOpMirrorDependent(p("2014"), p("01"), p("01"))
    val anotherDependentView = ProductBrandsNoOpMirrorDependent(p("2014"), p("01"), p("02"))
  }

  "A NoOp intermediate view in CreatedByViewManager state with existing _SUCCESS flag" should "transition to Waiting upon materialize" in new NoOpIntermediateView with SuccessFlag {
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

  it should "ask its dependencies to materialize" in new NoOpIntermediateView with SuccessFlag {
    val startState = CreatedByViewManager(viewUnderTest)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) =>
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, DEFAULT))
        }
      case _ => fail()
    }
  }

  it should "remember the materialization mode" in new NoOpIntermediateView with SuccessFlag {
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

  it should "pass the materialization mode to the dependencies" in new NoOpIntermediateView with SuccessFlag {
    val startState = CreatedByViewManager(viewUnderTest)

    stateMachine.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) =>
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, RESET_TRANSFORMATION_CHECKSUMS))
        }
      case _ => fail()
    }
  }

  "A NoOp intermediate view in CreatedByViewManager state with no _SUCCESS flag" should "transition to Waiting upon materialize" in new NoOpIntermediateView with NoSuccessFlag {
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

  it should "ask its dependencies to materialize" in new NoOpIntermediateView with NoSuccessFlag {
    val startState = CreatedByViewManager(viewUnderTest)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) =>
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, DEFAULT))
        }
      case _ => fail()
    }
  }

  it should "remember the materialization mode" in new NoOpIntermediateView with NoSuccessFlag {
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

  it should "pass the materialization mode to the dependencies" in new NoOpIntermediateView with NoSuccessFlag {
    val startState = CreatedByViewManager(viewUnderTest)

    stateMachine.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) =>
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, RESET_TRANSFORMATION_CHECKSUMS))
        }
      case _ => fail()
    }
  }

  "A NoOp intermediate view in Invalidated state with existing _SUCCESS flag" should "transition to Waiting upon materialize" in new NoOpIntermediateView with SuccessFlag {
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

  it should "ask its dependencies to materialize" in new NoOpIntermediateView with SuccessFlag {
    val startState = Invalidated(viewUnderTest)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) =>
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, DEFAULT))
        }
      case _ => fail()
    }
  }

  it should "remember the materialization mode" in new NoOpIntermediateView with SuccessFlag {
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

  it should "pass the materialization mode to the dependencies" in new NoOpIntermediateView with SuccessFlag {
    val startState = Invalidated(viewUnderTest)

    stateMachine.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) =>
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, RESET_TRANSFORMATION_CHECKSUMS))
        }
      case _ => fail()
    }
  }

  "A NoOp intermediate view in Invalidated state with no _SUCCESS flag" should "transition to Waiting upon materialize" in new NoOpIntermediateView with NoSuccessFlag {
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

  it should "ask its dependencies to materialize" in new NoOpIntermediateView with NoSuccessFlag {
    val startState = NoData(viewUnderTest)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) =>
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, DEFAULT))
        }
      case _ => fail()
    }
  }

  it should "remember the materialization mode" in new NoOpIntermediateView with NoSuccessFlag {
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

  it should "pass the materialization mode to the dependencies" in new NoOpIntermediateView with NoSuccessFlag {
    val startState = Invalidated(viewUnderTest)

    stateMachine.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) =>
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, RESET_TRANSFORMATION_CHECKSUMS))
        }
      case _ => fail()
    }
  }

  "A NoOp intermediate view in ReadFromSchemaManager state with existing _SUCCESS flag" should "transition to Waiting upon materialize" in new NoOpIntermediateView with SuccessFlag {
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

  it should "ask its dependencies to materialize" in new NoOpIntermediateView with SuccessFlag {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) =>
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, DEFAULT))
        }
      case _ => fail()
    }
  }

  it should "remember the materialization mode" in new NoOpIntermediateView with SuccessFlag {
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

  it should "pass the materialization mode to the dependencies" in new NoOpIntermediateView with SuccessFlag {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    stateMachine.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) =>
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, RESET_TRANSFORMATION_CHECKSUMS))
        }
      case _ => fail()
    }
  }

  "A NoOp intermediate view in ReadFromSchemaManager state with no _SUCCESS flag" should "transition to Waiting upon materialize" in new NoOpIntermediateView with NoSuccessFlag {
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

  it should "ask its dependencies to materialize" in new NoOpIntermediateView with NoSuccessFlag {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) =>
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, DEFAULT))
        }
      case _ => fail()
    }
  }

  it should "remember the materialization mode" in new NoOpIntermediateView with NoSuccessFlag {
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

  it should "pass the materialization mode to the dependencies" in new NoOpIntermediateView with NoSuccessFlag {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    stateMachine.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) =>
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, RESET_TRANSFORMATION_CHECKSUMS))
        }
      case _ => fail()
    }
  }

  "A NoOp intermediate view in NoData state with existing _SUCCESS flag" should "transition to Waiting upon materialize" in new NoOpIntermediateView with SuccessFlag {
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

  it should "ask its dependencies to materialize" in new NoOpIntermediateView with SuccessFlag {
    val startState = NoData(viewUnderTest)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) =>
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, DEFAULT))
        }
      case _ => fail()
    }
  }

  "A NoOp intermediate view in NoData state with no _SUCCESS flag" should "transition to Waiting upon materialize" in new NoOpIntermediateView with NoSuccessFlag {
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

  it should "ask its dependencies to materialize" in new NoOpIntermediateView with NoSuccessFlag {
    val startState = NoData(viewUnderTest)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) =>
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, DEFAULT))
        }
      case _ => fail()
    }
  }

  "A NoOp intermediate view in Waiting state with existing _SUCCESS flag" should "stay in Waiting adding another listener upon materialize" in new NoOpIntermediateView with SuccessFlag {
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

  "A NoOp intermediate view in Waiting state" should "stay waiting and report NotInvalidated upon invalidate" in new NoOpIntermediateView with SuccessFlag {
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

  "A NoOp intermediate view in Waiting state with no _SUCCESS flag" should "stay in Waiting adding another listener upon materialize" in new NoOpIntermediateView with NoSuccessFlag {
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

  "A NoOp intermediate view in Materialized state with existing _SUCCESS flag" should "transition to Waiting upon materialize" in new NoOpIntermediateView with SuccessFlag {
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

  it should "ask its dependencies to materialize" in new NoOpIntermediateView with SuccessFlag {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10, withErrors = false, incomplete = false)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) =>
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, DEFAULT))
        }
      case _ => fail()
    }
  }

  it should "remember the materialization mode" in new NoOpIntermediateView with SuccessFlag {
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

  it should "pass the materialization mode to the dependencies" in new NoOpIntermediateView with SuccessFlag {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10, withErrors = false, incomplete = false)

    stateMachine.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) =>
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, RESET_TRANSFORMATION_CHECKSUMS))
        }
      case _ => fail()
    }
  }

  "A NoOp intermediate view in Materialized state with no _SUCCESS flag" should "transition to Waiting upon materialize" in new NoOpIntermediateView with NoSuccessFlag {
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

  it should "ask its dependencies to materialize" in new NoOpIntermediateView with NoSuccessFlag {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10, withErrors = false, incomplete = false)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) =>
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, DEFAULT))
        }
      case _ => fail()
    }
  }

  it should "remember the materialization mode" in new NoOpIntermediateView with NoSuccessFlag {
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

  it should "pass the materialization mode to the dependencies" in new NoOpIntermediateView with NoSuccessFlag {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10, withErrors = false, incomplete = false)

    stateMachine.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) =>
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, RESET_TRANSFORMATION_CHECKSUMS))
        }
      case _ => fail()
    }
  }

  "A NoOp intermediate view in Waiting state receiving noDataAvailable" should "stay in Waiting if not all dependencies have answered yet" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      viewUnderTest.dependencies.toSet, Set(dependentView),
      DEFAULT, oneDependencyReturnedData = false, withErrors = false, incomplete = false, 0)

    stateMachine.noDataAvailable(startState, firstDependency, 20) match {
      case ResultingViewSchedulingState(
      Waiting(
      view,
      `viewTransformationChecksum`,
      10,
      waitingForDependencies, dependViews,
      DEFAULT,
      false, false, true, 0l
      ), _) =>
        waitingForDependencies shouldEqual Set(secondDependency)
        dependViews shouldEqual Set(DependentView(dependentView))
        view shouldBe viewUnderTest

      case _ => fail()
    }
  }

  it should "transition to and report NoDataAvailable if all dependencies answered noDataAvailable" in new NoOpIntermediateView with SuccessFlag {
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

  it should "transition to Materialize (incomplete) if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksums / timestamps are current" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 10)

    stateMachine.noDataAvailable(startState, firstDependency, 20) match {
      case ResultingViewSchedulingState(
      Materialized(
      view,
      `viewTransformationChecksum`,
      20,
      false,
      true),
      _) =>
        view shouldBe viewUnderTest

      case _ => fail()
    }
  }

  it should "report Materialized (incomplete) if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksums / timestamps are current" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 10)

    stateMachine.noDataAvailable(startState, firstDependency, 30) match {
      case ResultingViewSchedulingState(_, s) =>
        s shouldEqual Set(
          ReportMaterialized(
            viewUnderTest,
            Set(dependentView),
            20,
            withErrors = false,
            incomplete = true))

      case _ => fail()
    }
  }

  it should "write transformation checksum if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksums / timestamps are current, and RESET_TRANSFORMATION_CHECKSUM is set" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      RESET_TRANSFORMATION_CHECKSUMS, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 10)

    stateMachine.noDataAvailable(startState, firstDependency, 30) match {
      case ResultingViewSchedulingState(_, s) =>
        s should contain(WriteTransformationCheckum(viewUnderTest))

      case _ => fail()
    }
  }

  it should "report Materialized (incomplete) if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation timestamp is not current" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 10)

    stateMachine.noDataAvailable(startState, firstDependency, 30) match {
      case ResultingViewSchedulingState(_, s) =>
        s shouldEqual Set(
          ReportMaterialized(
            viewUnderTest,
            Set(dependentView),
            20,
            withErrors = false,
            incomplete = true))

      case _ => fail()
    }
  }

  it should "transition to Materialize (incomplete)  if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksum is not current" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, "outdated checksum", 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 10)

    stateMachine.noDataAvailable(startState, firstDependency, 30) match {
      case ResultingViewSchedulingState(
      Materialized(
      view,
      `viewTransformationChecksum`,
      30,
      false,
      true),
      _) =>
        view shouldBe viewUnderTest

      case _ => fail()
    }
  }

  it should "report Materialization (incomplete) if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksum is not current" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, "outdated checksum", 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 10)

    stateMachine.noDataAvailable(startState, firstDependency, 30) match {
      case ResultingViewSchedulingState(_, s) =>
        s should contain(
          ReportMaterialized(
            viewUnderTest,
            Set(dependentView),
            30,
            withErrors = false, incomplete = true))

      case _ => fail()
    }
  }

  it should "write transformation timestamp if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksum is not current" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, "outdated checksum", 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 10)

    stateMachine.noDataAvailable(startState, firstDependency, 30) match {
      case ResultingViewSchedulingState(_, s) =>
        s should contain(
          WriteTransformationTimestamp(viewUnderTest, 30))

      case _ => fail()
    }
  }

  it should "write transformation checksum if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksum is not current" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, "outdated checksum", 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 10)

    stateMachine.noDataAvailable(startState, firstDependency, 30) match {
      case ResultingViewSchedulingState(_, s) =>
        s should contain(
          WriteTransformationCheckum(viewUnderTest))

      case _ => fail()
    }
  }

  it should "transition to Materialize (incomplete)  if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation timestamp is not current" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 30)

    stateMachine.noDataAvailable(startState, firstDependency, 30) match {
      case ResultingViewSchedulingState(
      Materialized(
      view,
      `viewTransformationChecksum`,
      30,
      false,
      true),
      _) =>
        view shouldBe viewUnderTest

      case _ => fail()
    }
  }

  it should "report Materialization (incomplete)  if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation timestamp is not current" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 30)

    stateMachine.noDataAvailable(startState, firstDependency, 30) match {
      case ResultingViewSchedulingState(_, s) =>
        s should contain(
          ReportMaterialized(
            viewUnderTest,
            Set(dependentView),
            30,
            withErrors = false, incomplete = true))

      case _ => fail()
    }
  }

  it should "write transformation timestamp if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation timestamp is not current" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 30)

    stateMachine.noDataAvailable(startState, firstDependency, 30) match {
      case ResultingViewSchedulingState(_, s) =>
        s should contain(
          WriteTransformationTimestamp(viewUnderTest, 30))

      case _ => fail()
    }
  }

  it should "write transformation checksum if at least one dependency answered with data and a _SUCCESS flag exists and materialization mode is RESET_TRANSFORMATION_CHECKSUMS and the last transformation timestamp is not current" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      RESET_TRANSFORMATION_CHECKSUMS, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 30)

    stateMachine.noDataAvailable(startState, firstDependency, 30) match {
      case ResultingViewSchedulingState(_, s) =>
        s should contain(
          WriteTransformationCheckum(viewUnderTest))

      case _ => fail()
    }
  }

  "A NoOp intermediate view in Waiting state receiving failed" should "stay in Waiting if not all dependencies have answered yet" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      viewUnderTest.dependencies.toSet, Set(dependentView),
      DEFAULT, oneDependencyReturnedData = false, withErrors = false, incomplete = false, 0)

    stateMachine.failed(startState, firstDependency, 20) match {
      case ResultingViewSchedulingState(
      Waiting(
      view,
      `viewTransformationChecksum`,
      10,
      waitingForDependencies, dependViews,
      DEFAULT,
      false, true, true, 0l
      ), _) =>
        waitingForDependencies shouldEqual Set(secondDependency)
        dependViews shouldEqual Set(DependentView(dependentView))
        view shouldBe viewUnderTest

      case _ => fail()
    }
  }

  it should "transition to and report NoDataAvailable if no dependencies answered with Materialized" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = false, withErrors = false, incomplete = false, 0)

    stateMachine.failed(startState, firstDependency, 20) match {
      case ResultingViewSchedulingState(
      NoData(view),
      s) =>
        view shouldBe viewUnderTest
        s shouldEqual Set(ReportNoDataAvailable(viewUnderTest, Set(dependentView)))

      case _ => fail()
    }
  }

  it should "transition to Materialize (with errors / incomplete) if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksums / timestamps are current" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 10)

    stateMachine.failed(startState, firstDependency, 20) match {
      case ResultingViewSchedulingState(
      Materialized(
      view,
      `viewTransformationChecksum`,
      20,
      true,
      true),
      _) =>
        view shouldBe viewUnderTest

      case _ => fail()
    }
  }

  it should "report Materialization (with errors / incomplete) if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksum is not current" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, "outdated checksum", 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 10)

    stateMachine.failed(startState, firstDependency, 30) match {
      case ResultingViewSchedulingState(_, s) =>
        s should contain(
          ReportMaterialized(
            viewUnderTest,
            Set(dependentView),
            30,
            withErrors = true, incomplete = true))

      case _ => fail()
    }
  }

  it should "write transformation timestamp if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksum is not current" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, "outdated checksum", 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 10)

    stateMachine.failed(startState, firstDependency, 30) match {
      case ResultingViewSchedulingState(_, s) =>
        s should contain(
          WriteTransformationTimestamp(viewUnderTest, 30))

      case _ => fail()
    }
  }

  it should "write transformation checksum if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksum is not current" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, "outdated checksum", 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 10)

    stateMachine.failed(startState, firstDependency, 30) match {
      case ResultingViewSchedulingState(_, s) =>
        s should contain(
          WriteTransformationCheckum(viewUnderTest))

      case _ => fail()
    }
  }

  it should "transition to Materialize (incomplete / with errors)  if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation timestamp is not current" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 30)

    stateMachine.failed(startState, firstDependency, 30) match {
      case ResultingViewSchedulingState(
      Materialized(
      view,
      `viewTransformationChecksum`,
      30,
      true,
      true),
      _) =>
        view shouldBe viewUnderTest

      case _ => fail()
    }
  }

  it should "report Materialization (incomplete / with errors)  if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation timestamp is not current" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 30)

    stateMachine.failed(startState, firstDependency, 30) match {
      case ResultingViewSchedulingState(_, s) =>
        s should contain(
          ReportMaterialized(
            viewUnderTest,
            Set(dependentView),
            30,
            withErrors = true, incomplete = true))

      case _ => fail()
    }
  }

  it should "write transformation timestamp if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation timestamp is not current" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 30)

    stateMachine.failed(startState, firstDependency, 30) match {
      case ResultingViewSchedulingState(_, s) =>
        s should contain(
          WriteTransformationTimestamp(viewUnderTest, 30))

      case _ => fail()
    }
  }

  it should "write transformation checksum if at least one dependency answered with data and a _SUCCESS flag exists and materialization mode is RESET_TRANSFORMATION_CHECKSUMS and the last transformation timestamp is not current" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      RESET_TRANSFORMATION_CHECKSUMS, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 30)

    stateMachine.failed(startState, firstDependency, 30) match {
      case ResultingViewSchedulingState(_, s) =>
        s should contain(
          WriteTransformationCheckum(viewUnderTest))

      case _ => fail()
    }
  }

  "A NoOp intermediate view in Waiting state receiving materialized" should "stay in Waiting if not all dependencies have answered yet" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      viewUnderTest.dependencies.toSet, Set(dependentView),
      DEFAULT, oneDependencyReturnedData = false, withErrors = false, incomplete = false, 0)

    stateMachine.materialized(startState, firstDependency, 15, false, false, 20) match {
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

  it should "transition to and report NoDataAvailable if no dependencies answered with Materialized" in new NoOpIntermediateView with SuccessFlag {
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

  it should "transition to and report NoDataAvailable even if one dependency answered with data but no _SUCCESS flag exists" in new NoOpIntermediateView with NoSuccessFlag {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 0)

    stateMachine.materialized(startState, firstDependency, 15, false, false, 20) match {
      case ResultingViewSchedulingState(
      NoData(view),
      s) =>
        view shouldBe viewUnderTest
        s shouldEqual Set(ReportNoDataAvailable(viewUnderTest, Set(dependentView)))

      case _ => fail()
    }
  }

  it should "transition to Materialize if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksums / timestamps are current" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 10)

    stateMachine.materialized(startState, firstDependency, 15, false, false, 20) match {
      case ResultingViewSchedulingState(
      Materialized(
      view,
      `viewTransformationChecksum`,
      20,
      false,
      false),
      _) =>
        view shouldBe viewUnderTest

      case _ => fail()
    }
  }

  it should "report Materialization if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksum is not current" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, "outdated checksum", 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 10)

    stateMachine.materialized(startState, firstDependency, 15, false, false, 30) match {
      case ResultingViewSchedulingState(_, s) =>
        s should contain(
          ReportMaterialized(
            viewUnderTest,
            Set(dependentView),
            30,
            withErrors = false, incomplete = false))

      case _ => fail()
    }
  }

  it should "write transformation timestamp if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation timestamp is not current" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 30)

    stateMachine.materialized(startState, firstDependency, 15, false, false, 30) match {
      case ResultingViewSchedulingState(_, s) =>
        s should contain(
          WriteTransformationTimestamp(viewUnderTest, 30))

      case _ => fail()
    }
  }

  it should "write transformation checksum if at least one dependency answered with data and a _SUCCESS flag exists and materialization mode is RESET_TRANSFORMATION_CHECKSUMS and the last transformation timestamp is not current" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      RESET_TRANSFORMATION_CHECKSUMS, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 30)

    stateMachine.materialized(startState, firstDependency, 15, false, false, 30) match {
      case ResultingViewSchedulingState(_, s) =>
        s should contain(
          WriteTransformationCheckum(viewUnderTest))

      case _ => fail()
    }
  }

  it should "report Materialization (with errors / incomplete) if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksum is not current" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, "outdated checksum", 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 10)

    stateMachine.materialized(startState, firstDependency, 15, false, false, 30) match {
      case ResultingViewSchedulingState(_, s) =>
        s should contain(
          ReportMaterialized(
            viewUnderTest,
            Set(dependentView),
            30,
            withErrors = false, incomplete = false))

      case _ => fail()
    }
  }

  it should "write transformation timestamp if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksum is not current" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, "outdated checksum", 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 10)

    stateMachine.materialized(startState, firstDependency, 15, false, false, 30) match {
      case ResultingViewSchedulingState(_, s) =>
        s should contain(
          WriteTransformationTimestamp(viewUnderTest, 30))

      case _ => fail()
    }
  }

  it should "write transformation checksum if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksum is not current" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, "outdated checksum", 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = false, incomplete = false, 10)

    stateMachine.materialized(startState, firstDependency, 15, false, false, 30) match {
      case ResultingViewSchedulingState(_, s) =>
        s should contain(
          WriteTransformationCheckum(viewUnderTest))

      case _ => fail()
    }
  }

  it should "keep the error / incomplete settings when materializing" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = true, incomplete = true, 10)

    stateMachine.materialized(startState, firstDependency, 15, false, false, 20) match {
      case ResultingViewSchedulingState(
      Materialized(
      view,
      `viewTransformationChecksum`,
      20,
      true,
      true),
      _) =>
        view shouldBe viewUnderTest

      case _ => fail()
    }
  }

  it should "report report the error / incomplete settings when materializing" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, "outdated checksum", 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, oneDependencyReturnedData = true, withErrors = true, incomplete = true, 10)

    stateMachine.materialized(startState, firstDependency, 15, false, false, 30) match {
      case ResultingViewSchedulingState(_, s) =>
        s should contain(
          ReportMaterialized(
            viewUnderTest,
            Set(dependentView),
            30,
            withErrors = true, incomplete = true))

      case _ => fail()
    }
  }

  it should "leave incomplete / error settings untouched when remaining in waiting" in new NoOpIntermediateView with SuccessFlag {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      viewUnderTest.dependencies.toSet, Set(dependentView),
      DEFAULT, oneDependencyReturnedData = false, withErrors = true, incomplete = true, 0)

    stateMachine.materialized(startState, firstDependency, 15, false, false, 20) match {
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
}