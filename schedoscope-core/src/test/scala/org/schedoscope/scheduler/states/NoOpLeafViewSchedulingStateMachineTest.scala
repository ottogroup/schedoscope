package org.schedoscope.scheduler.states

import org.scalatest.{ FlatSpec, Matchers }
import test.eci.datahub.Brand
import org.schedoscope.dsl.Parameter.p
import test.eci.datahub.ProductBrand
import PartyInterestedInViewSchedulingStateChange._
import ViewSchedulingStateMachine._

class NoOpLeafViewSchedulingStateMachineTest extends FlatSpec with Matchers {

  trait NoOpLeafView {
    val dependentView = ProductBrand(p("EC0101"), p("2014"), p("01"), p("01"))
    val viewUnderTest = dependentView.brand()
    val viewTransformationChecksum = viewUnderTest.transformation().checksum
  }

  "A NoOp leaf view in CreatedByViewManager state with existing _SUCCESS flag" should "transition to Materialized upon materialize" in new NoOpLeafView {
    val startState = CreatedByViewManager(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(Materialized(v, viewTransformationChecksum, 10), _) => v shouldBe viewUnderTest
      case _ => fail()
    }
  }

  it should "request writing of new transformation checksum" in new NoOpLeafView {
    val startState = CreatedByViewManager(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true) match {
      case ResultingViewSchedulingState(_, s) => s should contain(WriteTransformationCheckum(viewUnderTest))
      case _                                  => fail()
    }
  }

  it should "request writing of new transformation time stamp" in new NoOpLeafView {
    val startState = CreatedByViewManager(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(_, s) => s should contain(WriteTransformationTimestamp(viewUnderTest, 10))
      case _                                  => fail()
    }
  }

  it should "report materialization to issuer" in new NoOpLeafView {
    val startState = CreatedByViewManager(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(_, s) => s should contain(ReportMaterialized(viewUnderTest, Set(dependentView), 10, false, false))
      case _                                  => fail()
    }
  }

  "A NoOp leaf view in CreatedByViewManager state with no _SUCCESS flag" should "transition to NoData upon materialize" in new NoOpLeafView {
    val startState = CreatedByViewManager(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = false) match {
      case ResultingViewSchedulingState(NoData(v), _) => v shouldBe viewUnderTest
      case _ => fail()
    }
  }

  it should "report no data available to issuer" in new NoOpLeafView {
    val startState = CreatedByViewManager(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = false, currentTime = 10) match {
      case ResultingViewSchedulingState(_, s) => s should contain(ReportNoDataAvailable(viewUnderTest, Set(dependentView)))
      case _                                  => fail()
    }
  }

  "A NoOp leaf view in Materialized state with existing _SUCCESS flag" should "remain Materialized upon materialize" in new NoOpLeafView {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 20) match {
      case ResultingViewSchedulingState(Materialized(v, viewTransformationChecksum, 10), _) => v shouldBe viewUnderTest
      case _ => fail()
    }
  }

  it should "report materialization to issuer" in new NoOpLeafView {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 20) match {
      case ResultingViewSchedulingState(_, s) => s should contain(ReportMaterialized(viewUnderTest, Set(dependentView), 10, false, false))
      case _                                  => fail()
    }
  }

  "A NoOp leaf view in Materialized state without _SUCCESS flag" should "remain Materialized upon materialize" in new NoOpLeafView {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = false, currentTime = 20) match {
      case ResultingViewSchedulingState(Materialized(v, viewTransformationChecksum, 10), _) => v shouldBe viewUnderTest
      case _ => fail()
    }
  }

  it should "report materialization to issuer" in new NoOpLeafView {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = false, currentTime = 20) match {
      case ResultingViewSchedulingState(_, s) => s should contain(ReportMaterialized(viewUnderTest, Set(dependentView), 10, false, false))
      case _                                  => fail()
    }
  }

  "A NoOp leaf view in NoData state with existing _SUCCESS flag" should "transition to Materialized upon materialize" in new NoOpLeafView {
    val startState = NoData(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(Materialized(v, viewTransformationChecksum, 10), _) => v shouldBe viewUnderTest
      case _ => fail()
    }
  }

  it should "request writing of new transformation checksum" in new NoOpLeafView {
    val startState = NoData(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true) match {
      case ResultingViewSchedulingState(_, s) => s should contain(WriteTransformationCheckum(viewUnderTest))
      case _                                  => fail()
    }
  }

  it should "request writing of new transformation time stamp" in new NoOpLeafView {
    val startState = NoData(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(_, s) => s should contain(WriteTransformationTimestamp(viewUnderTest, 10))
      case _                                  => fail()
    }
  }

  it should "report materialization to issuer" in new NoOpLeafView {
    val startState = NoData(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(_, s) => s should contain(ReportMaterialized(viewUnderTest, Set(dependentView), 10, false, false))
      case _                                  => fail()
    }
  }

  "A NoOp leaf view in NoData state with no _SUCCESS flag" should "stay in NoData upon materialize" in new NoOpLeafView {
    val startState = NoData(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = false) match {
      case ResultingViewSchedulingState(NoData(v), _) => v shouldBe viewUnderTest
      case _ => fail()
    }
  }

  it should "report no data available to issuer" in new NoOpLeafView {
    val startState = NoData(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = false, currentTime = 10) match {
      case ResultingViewSchedulingState(_, s) => s should contain(ReportNoDataAvailable(viewUnderTest, Set(dependentView)))
      case _                                  => fail()
    }
  }

  "A NoOp leaf view in ReadFromSchemaManager state with existing _SUCCESS flag" should "transition to Materialized upon materialize" in new NoOpLeafView {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 20) match {
      case ResultingViewSchedulingState(Materialized(v, viewTransformationChecksum, 10), _) => v shouldBe viewUnderTest
      case _ => fail()
    }
  }

  it should "report materialization to issuer" in new NoOpLeafView {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 20) match {
      case ResultingViewSchedulingState(_, s) => s should contain(ReportMaterialized(viewUnderTest, Set(dependentView), 10, false, false))
      case _                                  => fail()
    }
  }

  "A NoOp leaf view in ReadFromSchemaManager state with no _SUCCESS flag" should "transition to Materialized upon materialize" in new NoOpLeafView {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = false, currentTime = 20) match {
      case ResultingViewSchedulingState(Materialized(v, viewTransformationChecksum, 10), _) => v shouldBe viewUnderTest
      case _ => fail()
    }
  }

  it should "report materialization to issuer" in new NoOpLeafView {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = false, currentTime = 20) match {
      case ResultingViewSchedulingState(_, s) => s should contain(ReportMaterialized(viewUnderTest, Set(dependentView), 10, false, false))
      case _                                  => fail()
    }
  }

  "A NoOp leaf view in Invalidated state with existing _SUCCESS flag" should "transition to Materialized upon materialize" in new NoOpLeafView {
    val startState = Invalidated(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(Materialized(v, viewTransformationChecksum, 10), _) => v shouldBe viewUnderTest
      case _ => fail()
    }
  }

  it should "request writing of new transformation checksum" in new NoOpLeafView {
    val startState = Invalidated(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true) match {
      case ResultingViewSchedulingState(_, s) => s should contain(WriteTransformationCheckum(viewUnderTest))
      case _                                  => fail()
    }
  }

  it should "request writing of new transformation time stamp" in new NoOpLeafView {
    val startState = Invalidated(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(_, s) => s should contain(WriteTransformationTimestamp(viewUnderTest, 10))
      case _                                  => fail()
    }
  }

  it should "report materialization to issuer" in new NoOpLeafView {
    val startState = Invalidated(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case ResultingViewSchedulingState(_, s) => s should contain(ReportMaterialized(viewUnderTest, Set(dependentView), 10, false, false))
      case _                                  => fail()
    }
  }

  "A NoOp leaf view in Invalidated state with no _SUCCESS flag" should "transition to NoData upon materialize" in new NoOpLeafView {
    val startState = Invalidated(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = false) match {
      case ResultingViewSchedulingState(NoData(v), _) => v shouldBe viewUnderTest
      case _ => fail()
    }
  }

  it should "report no data available to issuer" in new NoOpLeafView {
    val startState = Invalidated(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = false, currentTime = 10) match {
      case ResultingViewSchedulingState(_, s) => s should contain(ReportNoDataAvailable(viewUnderTest, Set(dependentView)))
      case _                                  => fail()
    }
  }
}