package org.schedoscope.scheduler.states

import org.scalatest.{ FlatSpec, Matchers }
import PartyInterestedInViewSchedulingStateChange._
import ViewSchedulingStateMachine._
import org.schedoscope.scheduler.messages.MaterializeViewMode._
import org.schedoscope.dsl.Parameter.p
import test.eci.datahub.ProductBrandsNoOpMirror
import test.eci.datahub.ProductBrandsNoOpMirrorDependent

class NoOpIntermediateViewSchedulingStateMachineTest extends FlatSpec with Matchers {

  trait NoOpIntermediateView {
    val viewUnderTest = ProductBrandsNoOpMirror(p("2014"), p("01"), p("01"))
    val firstDependency = viewUnderTest.dependencies(0)
    val secondDependency = viewUnderTest.dependencies(1)
    val viewTransformationChecksum = viewUnderTest.transformation().checksum
    val dependentView = ProductBrandsNoOpMirrorDependent(p("2014"), p("01"), p("01"))
    val anotherDependentView = ProductBrandsNoOpMirrorDependent(p("2014"), p("01"), p("02"))
  }

  "A NoOp intermediate view in CreatedByViewManager state with existing _SUCCESS flag" should "transition to Waiting upon materialize" in new NoOpIntermediateView {
    val startState = CreatedByViewManager(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          viewUnderTest, org.schedoscope.dsl.transformations.Checksum.defaultDigest, 0,
          dependenciesMaterializing, interestedParties,
          DEFAULT, false, false, false, 0), _) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        interestedParties should contain(DependentView(dependentView))
      }
      case _ => fail()
    }
  }

  it should "ask its dependencies to materialize" in new NoOpIntermediateView {
    val startState = CreatedByViewManager(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) => {
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest, DEFAULT))
        }
      }
      case _ => fail()
    }
  }

  it should "remember the materialization mode" in new NoOpIntermediateView {
    val startState = CreatedByViewManager(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          _, _, _, _, _,
          materializationMode,
          _, _, _, _), _) => {
        materializationMode shouldBe RESET_TRANSFORMATION_CHECKSUMS
      }
      case _ => fail()
    }
  }

  it should "pass the materialization mode to the dependencies" in new NoOpIntermediateView {
    val startState = CreatedByViewManager(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) => {
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest, RESET_TRANSFORMATION_CHECKSUMS))
        }
      }
      case _ => fail()
    }
  }

  "A NoOp intermediate view in CreatedByViewManager state with no _SUCCESS flag" should "transition to Waiting upon materialize" in new NoOpIntermediateView {
    val startState = CreatedByViewManager(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = false, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          viewUnderTest, org.schedoscope.dsl.transformations.Checksum.defaultDigest, 0,
          dependenciesMaterializing, interestedParties,
          DEFAULT, false, false, false, 0), _) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        interestedParties should contain(DependentView(dependentView))
      }
      case _ => fail()
    }
  }

  it should "ask its dependencies to materialize" in new NoOpIntermediateView {
    val startState = CreatedByViewManager(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = false, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) => {
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest, DEFAULT))
        }
      }
      case _ => fail()
    }
  }

  it should "remember the materialization mode" in new NoOpIntermediateView {
    val startState = CreatedByViewManager(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, successFlagExists = false, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          _, _, _, _, _,
          materializationMode,
          _, _, _, _), _) => {
        materializationMode shouldBe RESET_TRANSFORMATION_CHECKSUMS
      }
      case _ => fail()
    }
  }

  it should "pass the materialization mode to the dependencies" in new NoOpIntermediateView {
    val startState = CreatedByViewManager(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, successFlagExists = false, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) => {
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest, RESET_TRANSFORMATION_CHECKSUMS))
        }
      }
      case _ => fail()
    }
  }

  "A NoOp intermediate view in Invalidated state with existing _SUCCESS flag" should "transition to Waiting upon materialize" in new NoOpIntermediateView {
    val startState = Invalidated(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          viewUnderTest, org.schedoscope.dsl.transformations.Checksum.defaultDigest, 0,
          dependenciesMaterializing, interestedParties,
          DEFAULT, false, false, false, 0), _) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        interestedParties should contain(DependentView(dependentView))
      }
      case _ => fail()
    }
  }

  it should "ask its dependencies to materialize" in new NoOpIntermediateView {
    val startState = Invalidated(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) => {
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest, DEFAULT))
        }
      }
      case _ => fail()
    }
  }

  it should "remember the materialization mode" in new NoOpIntermediateView {
    val startState = Invalidated(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          _, _, _, _, _,
          materializationMode,
          _, _, _, _), _) => {
        materializationMode shouldBe RESET_TRANSFORMATION_CHECKSUMS
      }
      case _ => fail()
    }
  }

  it should "pass the materialization mode to the dependencies" in new NoOpIntermediateView {
    val startState = Invalidated(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) => {
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest, RESET_TRANSFORMATION_CHECKSUMS))
        }
      }
      case _ => fail()
    }
  }

  "A NoOp intermediate view in Invalidated state with no _SUCCESS flag" should "transition to Waiting upon materialize" in new NoOpIntermediateView {
    val startState = NoData(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = false, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          viewUnderTest, org.schedoscope.dsl.transformations.Checksum.defaultDigest, 0,
          dependenciesMaterializing, interestedParties,
          DEFAULT, false, false, false, 0), _) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        interestedParties should contain(DependentView(dependentView))
      }
      case _ => fail()
    }
  }

  it should "ask its dependencies to materialize" in new NoOpIntermediateView {
    val startState = NoData(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = false, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) => {
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest, DEFAULT))
        }
      }
      case _ => fail()
    }
  }

  it should "remember the materialization mode" in new NoOpIntermediateView {
    val startState = Invalidated(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, successFlagExists = false, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          _, _, _, _, _,
          materializationMode,
          _, _, _, _), _) => {
        materializationMode shouldBe RESET_TRANSFORMATION_CHECKSUMS
      }
      case _ => fail()
    }
  }

  it should "pass the materialization mode to the dependencies" in new NoOpIntermediateView {
    val startState = Invalidated(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, successFlagExists = false, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) => {
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest, RESET_TRANSFORMATION_CHECKSUMS))
        }
      }
      case _ => fail()
    }
  }

  "A NoOp intermediate view in ReadFromSchemaManager state with existing _SUCCESS flag" should "transition to Waiting upon materialize" in new NoOpIntermediateView {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 20) match {
      case ResultingViewSchedulingState(
        Waiting(
          viewUnderTest, viewTransformationChecksum, 10,
          dependenciesMaterializing, interestedParties,
          DEFAULT, false, false, false, 0), _) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        interestedParties should contain(DependentView(dependentView))
      }
      case _ => fail()
    }
  }

  it should "ask its dependencies to materialize" in new NoOpIntermediateView {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) => {
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest, DEFAULT))
        }
      }
      case _ => fail()
    }
  }

  it should "remember the materialization mode" in new NoOpIntermediateView {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    viewUnderTest.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          _, _, _, _, _,
          materializationMode,
          _, _, _, _), _) => {
        materializationMode shouldBe RESET_TRANSFORMATION_CHECKSUMS
      }
      case _ => fail()
    }
  }

  it should "pass the materialization mode to the dependencies" in new NoOpIntermediateView {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    viewUnderTest.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) => {
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest, RESET_TRANSFORMATION_CHECKSUMS))
        }
      }
      case _ => fail()
    }
  }

  "A NoOp intermediate view in ReadFromSchemaManager state with no _SUCCESS flag" should "transition to Waiting upon materialize" in new NoOpIntermediateView {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = false, currentTime = 20) match {
      case ResultingViewSchedulingState(
        Waiting(
          viewUnderTest, viewTransformationChecksum, 10,
          dependenciesMaterializing, interestedParties,
          DEFAULT, false, false, false, 0), _) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        interestedParties should contain(DependentView(dependentView))
      }
      case _ => fail()
    }
  }

  it should "ask its dependencies to materialize" in new NoOpIntermediateView {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = false, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) => {
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest, DEFAULT))
        }
      }
      case _ => fail()
    }
  }

  it should "remember the materialization mode" in new NoOpIntermediateView {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    viewUnderTest.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, successFlagExists = false, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          _, _, _, _, _,
          materializationMode,
          _, _, _, _), _) => {
        materializationMode shouldBe RESET_TRANSFORMATION_CHECKSUMS
      }
      case _ => fail()
    }
  }

  it should "pass the materialization mode to the dependencies" in new NoOpIntermediateView {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    viewUnderTest.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, successFlagExists = false, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) => {
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest, RESET_TRANSFORMATION_CHECKSUMS))
        }
      }
      case _ => fail()
    }
  }

  "A NoOp intermediate view in NoData state with existing _SUCCESS flag" should "transition to Waiting upon materialize" in new NoOpIntermediateView {
    val startState = NoData(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          viewUnderTest, org.schedoscope.dsl.transformations.Checksum.defaultDigest, 0,
          dependenciesMaterializing, interestedParties,
          DEFAULT, false, false, false, 0), _) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        interestedParties should contain(DependentView(dependentView))
      }
      case _ => fail()
    }
  }

  it should "ask its dependencies to materialize" in new NoOpIntermediateView {
    val startState = NoData(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) => {
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest, DEFAULT))
        }
      }
      case _ => fail()
    }
  }

  "A NoOp intermediate view in NoData state with no _SUCCESS flag" should "transition to Waiting upon materialize" in new NoOpIntermediateView {
    val startState = NoData(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = false, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          viewUnderTest, org.schedoscope.dsl.transformations.Checksum.defaultDigest, 0,
          dependenciesMaterializing, interestedParties,
          DEFAULT, false, false, false, 0), _) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        interestedParties should contain(DependentView(dependentView))
      }
      case _ => fail()
    }
  }

  it should "ask its dependencies to materialize" in new NoOpIntermediateView {
    val startState = NoData(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = false, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) => {
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest, DEFAULT))
        }
      }
      case _ => fail()
    }
  }

  "A NoOp intermediate view in Waiting state with existing _SUCCESS flag" should "stay in Waiting adding another listener upon materialize" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      viewUnderTest.dependencies.toSet, Set(anotherDependentView),
      DEFAULT, false, false, false, 0)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          viewUnderTest, viewTransformationChecksum, 10,
          dependenciesMaterializing, interestedParties,
          DEFAULT, false, false, false, 0), s) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        interestedParties should contain(DependentView(dependentView))
        interestedParties should contain(DependentView(anotherDependentView))
        s shouldBe 'empty
      }
      case _ => fail()
    }
  }

  "A NoOp intermediate view in Waiting state" should "stay waiting and report NotInvalidated upon invalidate" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      viewUnderTest.dependencies.toSet, Set(anotherDependentView),
      DEFAULT, false, false, false, 0)

    viewUnderTest.invalidate(startState, dependentView) match {

      case ResultingViewSchedulingState(resultState, s) => {
        resultState shouldEqual startState
        s shouldEqual Set(ReportNotInvalidated(viewUnderTest, Set(dependentView)))
      }
      case _ => fail()
    }
  }

  "A NoOp intermediate view in Waiting state with no _SUCCESS flag" should "stay in Waiting adding another listener upon materialize" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      viewUnderTest.dependencies.toSet, Set(anotherDependentView),
      DEFAULT, false, false, false, 0)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = false, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          viewUnderTest, viewTransformationChecksum, 10,
          dependenciesMaterializing, interestedParties,
          DEFAULT, false, false, false, 0), s) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        interestedParties should contain(DependentView(dependentView))
        interestedParties should contain(DependentView(anotherDependentView))
        s shouldBe 'empty
      }
      case _ => fail()
    }
  }

  "A NoOp intermediate view in Materialized state with existing _SUCCESS flag" should "transition to Waiting upon materialize" in new NoOpIntermediateView {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10, false, false)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 20) match {
      case ResultingViewSchedulingState(
        Waiting(
          viewUnderTest, viewTransformationChecksum, 10,
          dependenciesMaterializing, interestedParties,
          DEFAULT, false, false, false, 0), _) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        interestedParties should contain(DependentView(dependentView))
      }
      case _ => fail()
    }
  }

  it should "ask its dependencies to materialize" in new NoOpIntermediateView {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10, false, false)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) => {
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest, DEFAULT))
        }
      }
      case _ => fail()
    }
  }

  it should "remember the materialization mode" in new NoOpIntermediateView {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10, false, false)

    viewUnderTest.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          _, _, _, _, _,
          materializationMode,
          _, _, _, _), _) => {
        materializationMode shouldBe RESET_TRANSFORMATION_CHECKSUMS
      }
      case _ => fail()
    }
  }

  it should "pass the materialization mode to the dependencies" in new NoOpIntermediateView {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10, false, false)

    viewUnderTest.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) => {
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest, RESET_TRANSFORMATION_CHECKSUMS))
        }
      }
      case _ => fail()
    }
  }

  "A NoOp intermediate view in Materialized state with no _SUCCESS flag" should "transition to Waiting upon materialize" in new NoOpIntermediateView {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10, false, false)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = false, currentTime = 20) match {
      case ResultingViewSchedulingState(
        Waiting(
          viewUnderTest, viewTransformationChecksum, 10,
          dependenciesMaterializing, interestedParties,
          DEFAULT, false, false, false, 0), _) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        interestedParties should contain(DependentView(dependentView))
      }
      case _ => fail()
    }
  }

  it should "ask its dependencies to materialize" in new NoOpIntermediateView {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10, false, false)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = false, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) => {
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest, DEFAULT))
        }
      }
      case _ => fail()
    }
  }

  it should "remember the materialization mode" in new NoOpIntermediateView {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10, false, false)

    viewUnderTest.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, successFlagExists = false, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          _, _, _, _, _,
          materializationMode,
          _, _, _, _), _) => {
        materializationMode shouldBe RESET_TRANSFORMATION_CHECKSUMS
      }
      case _ => fail()
    }
  }

  it should "pass the materialization mode to the dependencies" in new NoOpIntermediateView {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10, false, false)

    viewUnderTest.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS, successFlagExists = false, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) => {
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest, RESET_TRANSFORMATION_CHECKSUMS))
        }
      }
      case _ => fail()
    }
  }

  "A NoOp intermediate view in Waiting state receiving noDataAvailable" should "stay in Waiting if not all dependencies have answered yet" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      viewUnderTest.dependencies.toSet, Set(dependentView),
      DEFAULT, false, false, false, 0)

    viewUnderTest.noDataAvailable(startState, firstDependency, false, 20) match {
      case ResultingViewSchedulingState(
        Waiting(
          view,
          viewTransformationChecksum,
          10,
          waitingForDependencies, dependViews,
          DEFAULT,
          false, false, true, 0l
          ), _) => {
        waitingForDependencies shouldEqual Set(secondDependency)
        dependViews shouldEqual Set(DependentView(dependentView))
        view shouldBe viewUnderTest
      }

      case _ => fail()
    }
  }

  it should "transition to and report NoDataAvailable if all dependencies answered noDataAvailable" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      Set(firstDependency), Set(dependentView),
      DEFAULT, false, false, false, 0)

    viewUnderTest.noDataAvailable(startState, firstDependency, false, 20) match {
      case ResultingViewSchedulingState(
        NoData(view),
        s) => {
        view shouldBe viewUnderTest
        s shouldEqual Set(ReportNoDataAvailable(viewUnderTest, Set(dependentView)))
      }

      case _ => fail()
    }
  }

  it should "transition to and report NoDataAvailable even if one dependency answered with data but no _SUCCESS flag exists" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 0)

    viewUnderTest.noDataAvailable(startState, firstDependency, false, 20) match {
      case ResultingViewSchedulingState(
        NoData(view),
        s) => {
        view shouldBe viewUnderTest
        s shouldEqual Set(ReportNoDataAvailable(viewUnderTest, Set(dependentView)))
      }

      case _ => fail()
    }
  }

  it should "transition to Materialize (incomplete) if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksums / timestamps are current" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 10)

    viewUnderTest.noDataAvailable(startState, firstDependency, true, 20) match {
      case ResultingViewSchedulingState(
        Materialized(
          view,
          viewTransformationChecksum,
          20,
          false,
          true),
        _) => {
        view shouldBe viewUnderTest
      }

      case _ => fail()
    }
  }

  it should "report Materialized (incomplete) if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksums / timestamps are current" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 10)

    viewUnderTest.noDataAvailable(startState, firstDependency, true, 30) match {
      case ResultingViewSchedulingState(_, s) => {
        s shouldEqual Set(
          ReportMaterialized(
            viewUnderTest,
            Set(dependentView),
            20,
            false,
            true))
      }

      case _ => fail()
    }
  }

  it should "write transformation checksum if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksums / timestamps are current, and RESET_TRANSFORMATION_CHECKSUM is set" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      RESET_TRANSFORMATION_CHECKSUMS, true, false, false, 10)

    viewUnderTest.noDataAvailable(startState, firstDependency, true, 30) match {
      case ResultingViewSchedulingState(_, s) => {
        s should contain(WriteTransformationCheckum(viewUnderTest))
      }

      case _ => fail()
    }
  }

  it should "report Materialized (incomplete) if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation timestamp is not current" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 10)

    viewUnderTest.noDataAvailable(startState, firstDependency, true, 30) match {
      case ResultingViewSchedulingState(_, s) => {
        s shouldEqual Set(
          ReportMaterialized(
            viewUnderTest,
            Set(dependentView),
            20,
            false,
            true))
      }

      case _ => fail()
    }
  }

  it should "transition to Materialize (incomplete)  if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksum is not current" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, "outdated checksum", 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 10)

    viewUnderTest.noDataAvailable(startState, firstDependency, true, 30) match {
      case ResultingViewSchedulingState(
        Materialized(
          view,
          viewTransformationChecksum,
          30,
          false,
          true),
        _) => {
        view shouldBe viewUnderTest
      }

      case _ => fail()
    }
  }

  it should "report Materialization (incomplete) if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksum is not current" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, "outdated checksum", 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 10)

    viewUnderTest.noDataAvailable(startState, firstDependency, true, 30) match {
      case ResultingViewSchedulingState(_, s) => {
        s should contain(
          ReportMaterialized(
            viewUnderTest,
            Set(dependentView),
            30,
            false, true))
      }

      case _ => fail()
    }
  }

  it should "write transformation timestamp if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksum is not current" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, "outdated checksum", 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 10)

    viewUnderTest.noDataAvailable(startState, firstDependency, true, 30) match {
      case ResultingViewSchedulingState(_, s) => {
        s should contain(
          WriteTransformationTimestamp(viewUnderTest, 30))
      }

      case _ => fail()
    }
  }

  it should "write transformation checksum if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksum is not current" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, "outdated checksum", 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 10)

    viewUnderTest.noDataAvailable(startState, firstDependency, true, 30) match {
      case ResultingViewSchedulingState(_, s) => {
        s should contain(
          WriteTransformationCheckum(viewUnderTest))
      }

      case _ => fail()
    }
  }

  it should "transition to Materialize (incomplete)  if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation timestamp is not current" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 30)

    viewUnderTest.noDataAvailable(startState, firstDependency, true, 30) match {
      case ResultingViewSchedulingState(
        Materialized(
          view,
          viewTransformationChecksum,
          30,
          false,
          true),
        _) => {
        view shouldBe viewUnderTest
      }

      case _ => fail()
    }
  }

  it should "report Materialization (incomplete)  if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation timestamp is not current" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 30)

    viewUnderTest.noDataAvailable(startState, firstDependency, true, 30) match {
      case ResultingViewSchedulingState(_, s) => {
        s should contain(
          ReportMaterialized(
            viewUnderTest,
            Set(dependentView),
            30,
            false, true))
      }

      case _ => fail()
    }
  }

  it should "write transformation timestamp if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation timestamp is not current" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 30)

    viewUnderTest.noDataAvailable(startState, firstDependency, true, 30) match {
      case ResultingViewSchedulingState(_, s) => {
        s should contain(
          WriteTransformationTimestamp(viewUnderTest, 30))
      }

      case _ => fail()
    }
  }

  it should "write transformation checksum if at least one dependency answered with data and a _SUCCESS flag exists and materialization mode is RESET_TRANSFORMATION_CHECKSUMS and the last transformation timestamp is not current" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      RESET_TRANSFORMATION_CHECKSUMS, true, false, false, 30)

    viewUnderTest.noDataAvailable(startState, firstDependency, true, 30) match {
      case ResultingViewSchedulingState(_, s) => {
        s should contain(
          WriteTransformationCheckum(viewUnderTest))
      }

      case _ => fail()
    }
  }

  "A NoOp intermediate view in Waiting state receiving failed" should "stay in Waiting if not all dependencies have answered yet" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      viewUnderTest.dependencies.toSet, Set(dependentView),
      DEFAULT, false, false, false, 0)

    viewUnderTest.failed(startState, firstDependency, false, 20) match {
      case ResultingViewSchedulingState(
        Waiting(
          view,
          viewTransformationChecksum,
          10,
          waitingForDependencies, dependViews,
          DEFAULT,
          false, true, true, 0l
          ), _) => {
        waitingForDependencies shouldEqual Set(secondDependency)
        dependViews shouldEqual Set(DependentView(dependentView))
        view shouldBe viewUnderTest
      }

      case _ => fail()
    }
  }

  it should "transition to and report NoDataAvailable if no dependencies answered with Materialized" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      Set(firstDependency), Set(dependentView),
      DEFAULT, false, false, false, 0)

    viewUnderTest.failed(startState, firstDependency, false, 20) match {
      case ResultingViewSchedulingState(
        NoData(view),
        s) => {
        view shouldBe viewUnderTest
        s shouldEqual Set(ReportNoDataAvailable(viewUnderTest, Set(dependentView)))
      }

      case _ => fail()
    }
  }

  it should "transition to and report NoDataAvailable even if one dependency answered with data but no _SUCCESS flag exists" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 0)

    viewUnderTest.failed(startState, firstDependency, false, 20) match {
      case ResultingViewSchedulingState(
        NoData(view),
        s) => {
        view shouldBe viewUnderTest
        s shouldEqual Set(ReportNoDataAvailable(viewUnderTest, Set(dependentView)))
      }

      case _ => fail()
    }
  }

  it should "transition to Materialize (with errors / incomplete) if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksums / timestamps are current" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 10)

    viewUnderTest.failed(startState, firstDependency, true, 20) match {
      case ResultingViewSchedulingState(
        Materialized(
          view,
          viewTransformationChecksum,
          20,
          true,
          true),
        _) => {
        view shouldBe viewUnderTest
      }

      case _ => fail()
    }
  }

  it should "report Materialization (with errors / incomplete) if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksum is not current" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, "outdated checksum", 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 10)

    viewUnderTest.failed(startState, firstDependency, true, 30) match {
      case ResultingViewSchedulingState(_, s) => {
        s should contain(
          ReportMaterialized(
            viewUnderTest,
            Set(dependentView),
            30,
            true, true))
      }

      case _ => fail()
    }
  }

  it should "write transformation timestamp if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksum is not current" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, "outdated checksum", 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 10)

    viewUnderTest.failed(startState, firstDependency, true, 30) match {
      case ResultingViewSchedulingState(_, s) => {
        s should contain(
          WriteTransformationTimestamp(viewUnderTest, 30))
      }

      case _ => fail()
    }
  }

  it should "write transformation checksum if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksum is not current" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, "outdated checksum", 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 10)

    viewUnderTest.failed(startState, firstDependency, true, 30) match {
      case ResultingViewSchedulingState(_, s) => {
        s should contain(
          WriteTransformationCheckum(viewUnderTest))
      }

      case _ => fail()
    }
  }

  it should "transition to Materialize (incomplete / with errors)  if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation timestamp is not current" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 30)

    viewUnderTest.failed(startState, firstDependency, true, 30) match {
      case ResultingViewSchedulingState(
        Materialized(
          view,
          viewTransformationChecksum,
          30,
          true,
          true),
        _) => {
        view shouldBe viewUnderTest
      }

      case _ => fail()
    }
  }

  it should "report Materialization (incomplete / with errors)  if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation timestamp is not current" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 30)

    viewUnderTest.failed(startState, firstDependency, true, 30) match {
      case ResultingViewSchedulingState(_, s) => {
        s should contain(
          ReportMaterialized(
            viewUnderTest,
            Set(dependentView),
            30,
            true, true))
      }

      case _ => fail()
    }
  }

  it should "write transformation timestamp if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation timestamp is not current" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 30)

    viewUnderTest.failed(startState, firstDependency, true, 30) match {
      case ResultingViewSchedulingState(_, s) => {
        s should contain(
          WriteTransformationTimestamp(viewUnderTest, 30))
      }

      case _ => fail()
    }
  }

  it should "write transformation checksum if at least one dependency answered with data and a _SUCCESS flag exists and materialization mode is RESET_TRANSFORMATION_CHECKSUMS and the last transformation timestamp is not current" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      RESET_TRANSFORMATION_CHECKSUMS, true, false, false, 30)

    viewUnderTest.failed(startState, firstDependency, true, 30) match {
      case ResultingViewSchedulingState(_, s) => {
        s should contain(
          WriteTransformationCheckum(viewUnderTest))
      }

      case _ => fail()
    }
  }

  "A NoOp intermediate view in Waiting state receiving materialized" should "stay in Waiting if not all dependencies have answered yet" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      viewUnderTest.dependencies.toSet, Set(dependentView),
      DEFAULT, false, false, false, 0)

    viewUnderTest.materialized(startState, firstDependency, 15, false, 20) match {
      case ResultingViewSchedulingState(
        Waiting(
          view,
          viewTransformationChecksum,
          10,
          waitingForDependencies, dependViews,
          DEFAULT,
          true, false, false, 15
          ), _) => {
        waitingForDependencies shouldEqual Set(secondDependency)
        dependViews shouldEqual Set(DependentView(dependentView))
        view shouldBe viewUnderTest
      }

      case _ => fail()
    }
  }

  it should "transition to and report NoDataAvailable if no dependencies answered with Materialized" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      Set(firstDependency), Set(dependentView),
      DEFAULT, false, false, false, 0)

    viewUnderTest.noDataAvailable(startState, firstDependency, false, 20) match {
      case ResultingViewSchedulingState(
        NoData(view),
        s) => {
        view shouldBe viewUnderTest
        s shouldEqual Set(ReportNoDataAvailable(viewUnderTest, Set(dependentView)))
      }

      case _ => fail()
    }
  }

  it should "transition to and report NoDataAvailable even if one dependency answered with data but no _SUCCESS flag exists" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 0)

    viewUnderTest.materialized(startState, firstDependency, 15, false, 20) match {
      case ResultingViewSchedulingState(
        NoData(view),
        s) => {
        view shouldBe viewUnderTest
        s shouldEqual Set(ReportNoDataAvailable(viewUnderTest, Set(dependentView)))
      }

      case _ => fail()
    }
  }

  it should "transition to Materialize if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksums / timestamps are current" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 10)

    viewUnderTest.materialized(startState, firstDependency, 15, true, 20) match {
      case ResultingViewSchedulingState(
        Materialized(
          view,
          viewTransformationChecksum,
          20,
          false,
          false),
        _) => {
        view shouldBe viewUnderTest
      }

      case _ => fail()
    }
  }

  it should "report Materialization if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksum is not current" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, "outdated checksum", 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 10)

    viewUnderTest.materialized(startState, firstDependency, 15, true, 30) match {
      case ResultingViewSchedulingState(_, s) => {
        s should contain(
          ReportMaterialized(
            viewUnderTest,
            Set(dependentView),
            30,
            false, false))
      }

      case _ => fail()
    }
  }

  it should "write transformation timestamp if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation timestamp is not current" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 30)

    viewUnderTest.materialized(startState, firstDependency, 15, true, 30) match {
      case ResultingViewSchedulingState(_, s) => {
        s should contain(
          WriteTransformationTimestamp(viewUnderTest, 30))
      }

      case _ => fail()
    }
  }

  it should "write transformation checksum if at least one dependency answered with data and a _SUCCESS flag exists and materialization mode is RESET_TRANSFORMATION_CHECKSUMS and the last transformation timestamp is not current" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      RESET_TRANSFORMATION_CHECKSUMS, true, false, false, 30)

    viewUnderTest.materialized(startState, firstDependency, 15, true, 30) match {
      case ResultingViewSchedulingState(_, s) => {
        s should contain(
          WriteTransformationCheckum(viewUnderTest))
      }

      case _ => fail()
    }
  }

  it should "report Materialization (with errors / incomplete) if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksum is not current" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, "outdated checksum", 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 10)

    viewUnderTest.materialized(startState, firstDependency, 15, true, 30) match {
      case ResultingViewSchedulingState(_, s) => {
        s should contain(
          ReportMaterialized(
            viewUnderTest,
            Set(dependentView),
            30,
            false, false))
      }

      case _ => fail()
    }
  }

  it should "write transformation timestamp if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksum is not current" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, "outdated checksum", 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 10)

    viewUnderTest.materialized(startState, firstDependency, 15, true, 30) match {
      case ResultingViewSchedulingState(_, s) => {
        s should contain(
          WriteTransformationTimestamp(viewUnderTest, 30))
      }

      case _ => fail()
    }
  }

  it should "write transformation checksum if at least one dependency answered with data and a _SUCCESS flag exists and the last transformation checksum is not current" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, "outdated checksum", 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 10)

    viewUnderTest.materialized(startState, firstDependency, 15, true, 30) match {
      case ResultingViewSchedulingState(_, s) => {
        s should contain(
          WriteTransformationCheckum(viewUnderTest))
      }

      case _ => fail()
    }
  }

  it should "keep the error / incomplete settings when materializing" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, true, true, 10)

    viewUnderTest.materialized(startState, firstDependency, 15, true, 20) match {
      case ResultingViewSchedulingState(
        Materialized(
          view,
          viewTransformationChecksum,
          20,
          true,
          true),
        _) => {
        view shouldBe viewUnderTest
      }

      case _ => fail()
    }
  }

  it should "report report the error / incomplete settings when materializing" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, "outdated checksum", 20,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, true, true, 10)

    viewUnderTest.materialized(startState, firstDependency, 15, true, 30) match {
      case ResultingViewSchedulingState(_, s) => {
        s should contain(
          ReportMaterialized(
            viewUnderTest,
            Set(dependentView),
            30,
            true, true))
      }

      case _ => fail()
    }
  }

  it should "leave incomplete / error settings untouched when remaining in waiting" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      viewUnderTest.dependencies.toSet, Set(dependentView),
      DEFAULT, false, true, true, 0)

    viewUnderTest.materialized(startState, firstDependency, 15, false, 20) match {
      case ResultingViewSchedulingState(
        Waiting(
          view,
          viewTransformationChecksum,
          10,
          waitingForDependencies, dependViews,
          DEFAULT,
          true, true, true, 15
          ), _) => {
        waitingForDependencies shouldEqual Set(secondDependency)
        dependViews shouldEqual Set(DependentView(dependentView))
        view shouldBe viewUnderTest
      }

      case _ => fail()
    }
  }
}