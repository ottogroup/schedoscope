package org.schedoscope.scheduler.states

import org.scalatest.{ FlatSpec, Matchers }
import PartyInterestedInViewSchedulingStateChange._
import ViewSchedulingStateMachine._
import org.schedoscope.scheduler.messages.MaterializeViewMode._
import org.schedoscope.dsl.Parameter.p
import test.eci.datahub.ProductBrand
import test.eci.datahub.ProductBrandsNoOpMirror

class BaseIntermediateViewSchedulingStateMachineTest extends FlatSpec with Matchers {

  trait IntermediateView {
    val dependentView = ProductBrandsNoOpMirror(p("2014"), p("01"), p("01"))
    val anotherDependentView = ProductBrandsNoOpMirror(p("2014"), p("01"), p("02"))
    val viewUnderTest = dependentView.dependencies(0)
    val firstDependency = viewUnderTest.dependencies(0)
    val secondDependency = viewUnderTest.dependencies(1)
    val materializeOnceView = anotherDependentView.dependencies(0)
    materializeOnceView.materializeOnce
    val viewTransformationChecksum = viewUnderTest.transformation().checksum
  }

  "An intermediate view in CreatedByViewManager state" should "transition to Waiting upon materialize" in new IntermediateView {
    val startState = CreatedByViewManager(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          viewUnderTest, org.schedoscope.dsl.transformations.Checksum.defaultDigest, 0,
          dependenciesMaterializing, listenersWaitingForMaterialize,
          DEFAULT, false, false, false, 0), _) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        listenersWaitingForMaterialize should contain(DependentView(dependentView))
      }
      case _ => fail()
    }
  }

  it should "ask its dependencies to materialize" in new IntermediateView {
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

  it should "remember the materialization mode" in new IntermediateView {
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

  it should "pass the materialization mode to the dependencies" in new IntermediateView {
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

  it should "transition to Invalidated upon invalidate" in new IntermediateView {
    val startState = CreatedByViewManager(viewUnderTest)

    viewUnderTest.invalidate(startState, dependentView) match {
      case ResultingViewSchedulingState(Invalidated(view), s) => {
        view shouldBe viewUnderTest
        s shouldEqual Set(ReportInvalidated(view, Set(dependentView)))
      }
    }
  }

  "An intermediate view in Invalidated state" should "transition to Waiting upon materialize" in new IntermediateView {
    val startState = Invalidated(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          viewUnderTest, org.schedoscope.dsl.transformations.Checksum.defaultDigest, 0,
          dependenciesMaterializing, listenersWaitingForMaterialize,
          DEFAULT, false, false, false, 0), _) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        listenersWaitingForMaterialize should contain(DependentView(dependentView))
      }
      case _ => fail()
    }
  }

  it should "ask its dependencies to materialize" in new IntermediateView {
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

  it should "remember the materialization mode" in new IntermediateView {
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

  it should "pass the materialization mode to the dependencies" in new IntermediateView {
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

  it should "stay in Invalidated upon invalidate" in new IntermediateView {
    val startState = Invalidated(viewUnderTest)

    viewUnderTest.invalidate(startState, dependentView) match {
      case ResultingViewSchedulingState(Invalidated(view), s) => {
        view shouldBe viewUnderTest
        s shouldEqual Set(ReportInvalidated(view, Set(dependentView)))
      }
    }
  }

  "An intermediate view in NoData state" should "transition to Waiting upon materialize" in new IntermediateView {
    val startState = NoData(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          viewUnderTest, org.schedoscope.dsl.transformations.Checksum.defaultDigest, 0,
          dependenciesMaterializing, listenersWaitingForMaterialize,
          DEFAULT, false, false, false, 0), _) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        listenersWaitingForMaterialize should contain(DependentView(dependentView))
      }
      case _ => fail()
    }
  }

  it should "ask its dependencies to materialize" in new IntermediateView {
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

  it should "transition to Invalidated upon invalidate" in new IntermediateView {
    val startState = NoData(viewUnderTest)

    viewUnderTest.invalidate(startState, dependentView) match {
      case ResultingViewSchedulingState(Invalidated(view), s) => {
        view shouldBe viewUnderTest
        s shouldEqual Set(ReportInvalidated(view, Set(dependentView)))
      }
    }
  }

  "An intermediate view in ReadFromSchemaManager state" should "transition to Waiting upon materialize" in new IntermediateView {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 20) match {
      case ResultingViewSchedulingState(
        Waiting(
          viewUnderTest, viewTransformationChecksum, 10,
          dependenciesMaterializing, listenersWaitingForMaterialize,
          DEFAULT, false, false, false, 0), _) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        listenersWaitingForMaterialize should contain(DependentView(dependentView))
      }
      case _ => fail()
    }
  }

  it should "ask its dependencies to materialize" in new IntermediateView {
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

  it should "remember the materialization mode" in new IntermediateView {
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

  it should "pass the materialization mode to the dependencies" in new IntermediateView {
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

  it should "transition to Invalidated upon invalidate" in new IntermediateView {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    viewUnderTest.invalidate(startState, dependentView) match {
      case ResultingViewSchedulingState(Invalidated(view), s) => {
        view shouldBe viewUnderTest
        s shouldEqual Set(ReportInvalidated(view, Set(dependentView)))
      }
    }
  }

  it should "become Materialized upon materialize if materialize once is set" in new IntermediateView {
    val startState = ReadFromSchemaManager(materializeOnceView, viewTransformationChecksum, 10)

    materializeOnceView.materialize(startState, dependentView, successFlagExists = true, currentTime = 20) match {
      case ResultingViewSchedulingState(
        Materialized(
          view,
          viewTransformationChecksum,
          10,
          false,
          false), s) => {
        view shouldBe materializeOnceView
        s shouldEqual Set(
          ReportMaterialized(
            materializeOnceView,
            Set(dependentView),
            10,
            false,
            false))
      }

      case _ => fail()
    }
  }

  it should "become Materialized upon materialize if materialize once is set and write transformation checksum if RESET_TRANSFORMATION_CHECKSUMS is set" in new IntermediateView {
    val startState = ReadFromSchemaManager(materializeOnceView, viewTransformationChecksum, 10)

    materializeOnceView.materialize(startState, dependentView, successFlagExists = true, RESET_TRANSFORMATION_CHECKSUMS, currentTime = 20) match {
      case ResultingViewSchedulingState(
        Materialized(_, _, _, _, _), s) => {
        s should contain(WriteTransformationCheckum(materializeOnceView))
      }

      case _ => fail()
    }
  }

  "An intermediate view in Materialized state" should "transition to Waiting upon materialize" in new IntermediateView {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10, false, false)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 20) match {
      case ResultingViewSchedulingState(
        Waiting(
          viewUnderTest, viewTransformationChecksum, 10,
          dependenciesMaterializing, listenersWaitingForMaterialize,
          DEFAULT, false, false, false, 0), _) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        listenersWaitingForMaterialize should contain(DependentView(dependentView))
      }
      case _ => fail()
    }
  }

  it should "ask its dependencies to materialize" in new IntermediateView {
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

  it should "remember the materialization mode" in new IntermediateView {
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

  it should "pass the materialization mode to the dependencies" in new IntermediateView {
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

  it should "transition to Invalidated upon invalidate" in new IntermediateView {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10, false, false)

    viewUnderTest.invalidate(startState, dependentView) match {
      case ResultingViewSchedulingState(Invalidated(view), s) => {
        view shouldBe viewUnderTest
        s shouldEqual Set(ReportInvalidated(view, Set(dependentView)))
      }
    }
  }

  it should "stay Materialized upon materialize if materialize once is set" in new IntermediateView {
    val startState = Materialized(materializeOnceView, viewTransformationChecksum, 10, false, false)

    materializeOnceView.materialize(startState, dependentView, successFlagExists = true, currentTime = 20) match {
      case ResultingViewSchedulingState(
        Materialized(
          view,
          viewTransformationChecksum,
          10,
          false,
          false), s) => {
        view shouldBe materializeOnceView
        s shouldEqual Set(
          ReportMaterialized(
            materializeOnceView,
            Set(dependentView),
            10,
            false,
            false))
      }

      case _ => fail()
    }
  }

  it should "become Materialized upon materialize if materialize once is set and write transformation checksum if RESET_TRANSFORMATION_CHECKSUMS is set" in new IntermediateView {
    val startState = Materialized(materializeOnceView, viewTransformationChecksum, 10, false, false)

    materializeOnceView.materialize(startState, dependentView, successFlagExists = true, RESET_TRANSFORMATION_CHECKSUMS, currentTime = 20) match {
      case ResultingViewSchedulingState(
        Materialized(_, _, _, _, _), s) => {
        s should contain(WriteTransformationCheckum(materializeOnceView))
      }

      case _ => fail()
    }
  }

  "A NoOp intermediate view in Waiting state" should "stay in Waiting adding another listener upon materialize" in new IntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      viewUnderTest.dependencies.toSet, Set(anotherDependentView),
      DEFAULT, false, false, false, 0)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          viewUnderTest, viewTransformationChecksum, 10,
          dependenciesMaterializing, listenersWaitingForMaterialize,
          DEFAULT, false, false, false, 0), s) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        listenersWaitingForMaterialize should contain(DependentView(dependentView))
        listenersWaitingForMaterialize should contain(DependentView(anotherDependentView))
        s shouldBe 'empty
      }
      case _ => fail()
    }
  }

  it should "stay waiting and report NotInvalidated upon invalidate" in new IntermediateView {
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

  "An intermediate view in Waiting state receiving materialized" should "stay in Waiting if not all dependencies have answered yet" in new IntermediateView {
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

  it should "leave incomplete / error settings untouched when remaining in waiting" in new IntermediateView {
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

  it should "transition to and report NoDataAvailable if no dependencies answered with Materialized" in new IntermediateView {
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

  it should "transition to Transforming and demand transformation if the last dependency answers with materialized and is newer" in new IntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      Set(firstDependency), Set(dependentView),
      DEFAULT, false, false, false, 0)

    viewUnderTest.materialized(startState, firstDependency, 15, false, 20) match {
      case ResultingViewSchedulingState(
        Transforming(
          view,
          lastTransformationChecksum,
          listenersWaitingForMaterialize,
          DEFAULT,
          false,
          false,
          0
          ),
        s) => {
        listenersWaitingForMaterialize shouldEqual Set(DependentView(dependentView))
        view shouldBe viewUnderTest
        s shouldEqual Set(Transform(viewUnderTest))
      }

      case _ => fail()
    }
  }

  it should "transition to Transforming and demand transformation if at least one dependency has answered with materialized and it is older" in new IntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 15)

    viewUnderTest.noDataAvailable(startState, firstDependency, false, 20) match {
      case ResultingViewSchedulingState(
        Transforming(
          view,
          lastTransformationChecksum,
          listenersWaitingForMaterialize,
          DEFAULT,
          false,
          false,
          0
          ),
        s) => {
        listenersWaitingForMaterialize shouldEqual Set(DependentView(dependentView))
        view shouldBe viewUnderTest
        s shouldEqual Set(Transform(viewUnderTest))
      }

      case _ => fail()
    }
  }

  it should "skip Transforming and report Materialize when in mode RESET_TRANSFORMATION_CHECKSUMS_AND_TIMESTAMPS even if at least one dependency has answered with materialized and it is older" in new IntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      Set(firstDependency), Set(dependentView),
      RESET_TRANSFORMATION_CHECKSUMS_AND_TIMESTAMPS, true, false, false, 15)

    viewUnderTest.noDataAvailable(startState, firstDependency, false, 20) match {
      case ResultingViewSchedulingState(
        Materialized(
          view,
          viewTransformationChecksum,
          20,
          false,
          true), s) => {
        view shouldBe viewUnderTest
        s should contain(
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

  it should "skip Transforming and write transformation checksum when in mode RESET_TRANSFORMATION_CHECKSUMS_AND_TIMESTAMPS and the checksum has changed even if at least one dependency has answered with materialized and it is older" in new IntermediateView {
    val startState = Waiting(
      viewUnderTest, "outdated checksum", 10,
      Set(firstDependency), Set(dependentView),
      RESET_TRANSFORMATION_CHECKSUMS_AND_TIMESTAMPS, true, false, false, 15)

    viewUnderTest.noDataAvailable(startState, firstDependency, false, 20) match {
      case ResultingViewSchedulingState(
        Materialized(_, _, _, _, _), s) => {
        s should contain(
          WriteTransformationCheckum(viewUnderTest))
      }

      case _ => fail()
    }
  }

  it should "skip Transforming and not write transformation checksum when in mode RESET_TRANSFORMATION_CHECKSUMS_AND_TIMESTAMPS and the checksum has not changed even if at least one dependency has answered with materialized and it is older" in new IntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      Set(firstDependency), Set(dependentView),
      RESET_TRANSFORMATION_CHECKSUMS_AND_TIMESTAMPS, true, false, false, 15)

    viewUnderTest.noDataAvailable(startState, firstDependency, false, 20) match {
      case ResultingViewSchedulingState(
        Materialized(_, _, _, _, _), s) => {
        s should not(contain(
          WriteTransformationCheckum(viewUnderTest)))
      }

      case _ => fail()
    }
  }
  
  it should "skip Transforming and write transformation timestamp when in mode RESET_TRANSFORMATION_CHECKSUMS_AND_TIMESTAMPS even if at least one dependency has answered with materialized and it is older" in new IntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      Set(firstDependency), Set(dependentView),
      RESET_TRANSFORMATION_CHECKSUMS_AND_TIMESTAMPS, true, false, false, 15)

    viewUnderTest.noDataAvailable(startState, firstDependency, false, 20) match {
      case ResultingViewSchedulingState(
        Materialized(_, _, _, _, _), s) => {
        s should contain(
          WriteTransformationTimestamp(viewUnderTest, 20))
      }

      case _ => fail()
    }
  }

  it should "transition to Materialized if at least one dependency has answered with materialized and it is newer" in new IntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      Set(firstDependency), Set(dependentView),
      DEFAULT, true, false, false, 5)

    viewUnderTest.materialized(startState, firstDependency, 5, false, 20) match {
      case ResultingViewSchedulingState(
        Materialized(
          view,
          viewTransformationChecksum,
          10,
          false,
          false), s) => {
        view shouldBe viewUnderTest
        s shouldEqual Set(
          ReportMaterialized(
            viewUnderTest,
            Set(dependentView),
            10,
            false,
            false))
      }

      case _ => fail()
    }
  }

  it should "transition to Materialized and record transformation checksum if at least one dependency has answered with materialized and it is newer and RESET_TRANSFORMATION_CHECKSUMS is set" in new IntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      Set(firstDependency), Set(dependentView),
      RESET_TRANSFORMATION_CHECKSUMS, true, false, false, 5)

    viewUnderTest.materialized(startState, firstDependency, 5, false, 20) match {
      case ResultingViewSchedulingState(
        Materialized(_, _, _, _, _), s) => {
        s should contain(WriteTransformationCheckum(viewUnderTest))
      }

      case _ => fail()
    }
  }

}