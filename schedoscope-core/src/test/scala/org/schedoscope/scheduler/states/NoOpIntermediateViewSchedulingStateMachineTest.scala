package org.schedoscope.scheduler.states

import org.scalatest.{ FlatSpec, Matchers }
import PartyInterestedInViewSchedulingStateChange._
import ViewSchedulingStateMachine._
import org.schedoscope.scheduler.messages.MaterializeViewMode._
import org.schedoscope.dsl.Parameter.p
import test.eci.datahub.ProductBrandsNoOpMirror
import test.eci.datahub.ProductBrandsNoOpMirrorDependent
import org.schedoscope.dsl.transformations.Checksum.defaultDigest

class NoOpIntermediateViewSchedulingStateMachineTest extends FlatSpec with Matchers {

  trait NoOpIntermediateView {
    val viewUnderTest = ProductBrandsNoOpMirror(p("2014"), p("01"), p("01"))
    val viewTransformationChecksum = viewUnderTest.transformation().checksum
    val dependentView = ProductBrandsNoOpMirrorDependent(p("2014"), p("01"), p("01"))
    val anotherDependentView = ProductBrandsNoOpMirrorDependent(p("2014"), p("01"), p("02"))
  }

  "A NoOp intermediate view in CreatedByViewManager state with existing _SUCCESS flag" should "transition to Waiting upon materialize" in new NoOpIntermediateView {
    val startState = CreatedByViewManager(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          viewUnderTest, defaultDigest, 0,
          dependenciesMaterializing, listenersWaitingForMaterialize,
          DEFAULT, false, false, 0), _) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        listenersWaitingForMaterialize should contain(DependentView(dependentView))
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
          _, _, _), _) => {
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
          viewUnderTest, defaultDigest, 0,
          dependenciesMaterializing, listenersWaitingForMaterialize,
          DEFAULT, false, false, 0), _) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        listenersWaitingForMaterialize should contain(DependentView(dependentView))
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
          _, _, _), _) => {
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
          viewUnderTest, viewTransformationChecksum, 0,
          dependenciesMaterializing, listenersWaitingForMaterialize,
          DEFAULT, false, false, 0), _) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        listenersWaitingForMaterialize should contain(DependentView(dependentView))
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
          _, _, _), _) => {
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
          viewUnderTest, viewTransformationChecksum, 0,
          dependenciesMaterializing, listenersWaitingForMaterialize,
          DEFAULT, false, false, 0), _) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        listenersWaitingForMaterialize should contain(DependentView(dependentView))
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
          _, _, _), _) => {
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
          dependenciesMaterializing, listenersWaitingForMaterialize,
          DEFAULT, false, false, 0), _) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        listenersWaitingForMaterialize should contain(DependentView(dependentView))
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
          _, _, _), _) => {
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
          dependenciesMaterializing, listenersWaitingForMaterialize,
          DEFAULT, false, false, 0), _) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        listenersWaitingForMaterialize should contain(DependentView(dependentView))
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
          _, _, _), _) => {
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
          viewUnderTest, viewTransformationChecksum, 0,
          dependenciesMaterializing, listenersWaitingForMaterialize,
          DEFAULT, false, false, 0), _) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        listenersWaitingForMaterialize should contain(DependentView(dependentView))
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
          viewUnderTest, viewTransformationChecksum, 0,
          dependenciesMaterializing, listenersWaitingForMaterialize,
          DEFAULT, false, false, 0), _) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        listenersWaitingForMaterialize should contain(DependentView(dependentView))
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
      DEFAULT, false, false, 0)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          viewUnderTest, viewTransformationChecksum, 10,
          dependenciesMaterializing, listenersWaitingForMaterialize,
          DEFAULT, false, false, 0), s) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        listenersWaitingForMaterialize should contain(DependentView(dependentView))
        listenersWaitingForMaterialize should contain(DependentView(anotherDependentView))
        s shouldBe 'empty
      }
      case _ => fail()
    }
  }

  "A NoOp intermediate view in Waiting state with no _SUCCESS flag" should "stay in Waiting adding another listener upon materialize" in new NoOpIntermediateView {
    val startState = Waiting(
      viewUnderTest, viewTransformationChecksum, 10,
      viewUnderTest.dependencies.toSet, Set(anotherDependentView),
      DEFAULT, false, false, 0)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = false, currentTime = 10) match {
      case ResultingViewSchedulingState(
        Waiting(
          viewUnderTest, viewTransformationChecksum, 10,
          dependenciesMaterializing, listenersWaitingForMaterialize,
          DEFAULT, false, false, 0), s) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        listenersWaitingForMaterialize should contain(DependentView(dependentView))
        listenersWaitingForMaterialize should contain(DependentView(anotherDependentView))
        s shouldBe 'empty
      }
      case _ => fail()
    }
  }

  "A NoOp intermediate view in Materialitzed state with existing _SUCCESS flag" should "transition to Waiting upon materialize" in new NoOpIntermediateView {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10, false, false)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 20) match {
      case ResultingViewSchedulingState(
        Waiting(
          viewUnderTest, viewTransformationChecksum, 10,
          dependenciesMaterializing, listenersWaitingForMaterialize,
          DEFAULT, false, false, 0), _) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        listenersWaitingForMaterialize should contain(DependentView(dependentView))
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
          _, _, _), _) => {
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
          dependenciesMaterializing, listenersWaitingForMaterialize,
          DEFAULT, false, false, 0), _) => {
        dependenciesMaterializing shouldEqual viewUnderTest.dependencies.toSet
        listenersWaitingForMaterialize should contain(DependentView(dependentView))
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
          _, _, _), _) => {
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
}