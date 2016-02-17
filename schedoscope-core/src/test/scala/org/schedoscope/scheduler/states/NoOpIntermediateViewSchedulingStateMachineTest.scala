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
          s should contain(Materialize(d, viewUnderTest))
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
          s should contain(Materialize(d, viewUnderTest))
        }
      }
      case _ => fail()
    }
  }

  "A NoOp intermediate view in Invalidated state with existing _SUCCESS flag" should "transition to Materialized upon materialize" in new NoOpIntermediateView {
    val startState = Invalidated(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(Materialized(v, viewTransformationChecksum, 10), _) => v shouldBe viewUnderTest
      case _ => fail()
    }
  }

  it should "request writing of new transformation checksum" in new NoOpIntermediateView {
    val startState = Invalidated(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true) match {
      case ResultingViewSchedulingState(_, s) => s should contain(WriteTransformationCheckum(viewUnderTest))
      case _                                  => fail()
    }
  }

  it should "request writing of new transformation time stamp" in new NoOpIntermediateView {
    val startState = Invalidated(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(_, s) => s should contain(WriteTransformationTimestamp(viewUnderTest, 10))
      case _                                  => fail()
    }
  }

  it should "report materialization to issuer" in new NoOpIntermediateView {
    val startState = Invalidated(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(_, s) => s should contain(ReportMaterialized(viewUnderTest, Set(dependentView), 10, false, false))
      case _                                  => fail()
    }
  }

  "A NoOp intermediate view in Invalidated state with no _SUCCESS flag" should "transition to NoData upon materialize" in new NoOpIntermediateView {
    val startState = Invalidated(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = false) match {
      case ResultingViewSchedulingState(NoData(v), _) => v shouldBe viewUnderTest
      case _ => fail()
    }
  }

  it should "report no data available to issuer" in new NoOpIntermediateView {
    val startState = Invalidated(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = false, currentTime = 10) match {
      case ResultingViewSchedulingState(_, s) => s should contain(ReportNoDataAvailable(viewUnderTest, Set(dependentView)))
      case _                                  => fail()
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
          s should contain(Materialize(d, viewUnderTest))
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
          s should contain(Materialize(d, viewUnderTest))
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
          s should contain(Materialize(d, viewUnderTest))
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
          s should contain(Materialize(d, viewUnderTest))
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
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10)

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
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) => {
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest))
        }
      }
      case _ => fail()
    }
  }

  "A NoOp intermediate view in Materialitzed state with no _SUCCESS flag" should "transition to Waiting upon materialize" in new NoOpIntermediateView {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10)

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
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = false, currentTime = 10) match {
      case ResultingViewSchedulingState(__, s) => {
        viewUnderTest.dependencies.foreach { d =>
          s should contain(Materialize(d, viewUnderTest))
        }
      }
      case _ => fail()
    }
  }
}