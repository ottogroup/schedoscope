package org.schedoscope.scheduler.states

import org.scalatest.{ FlatSpec, Matchers }
import test.eci.datahub.Brand
import org.schedoscope.dsl.Parameter.p
import test.eci.datahub.ProductBrand
import ViewSchedulingStateListener._
import ViewSchedulingStateMachine._

class NoOpSchedulingStateMachineTest extends FlatSpec with Matchers {

  trait NoOpLeafView {
    val dependentView = ProductBrand(p("EC0101"), p("2014"), p("01"), p("01"))
    val viewUnderTest = dependentView.brand()
    val viewTransformationChecksum = viewUnderTest.transformation().checksum
  }
  
  "A NoOp leaf view in CreatedByViewManagerState with existing _SUCCESS flag" should "transition to Materialized upon materialize" in new NoOpLeafView {
    val startState = CreatedByViewManager(viewUnderTest)
    
    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case NewState(Materialized(v, viewTransformationChecksum, 10), _) => v shouldBe viewUnderTest
      case _                                  => fail()
    }
  }

  it should "request writing of new transformation checksum" in new NoOpLeafView {
    val startState = CreatedByViewManager(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true) match {
      case NewState(_, s) => s should contain(WriteTransformationCheckum(viewUnderTest))
      case _              => fail()
    }
  }

  it should "request writing of new transformation time stamp" in new NoOpLeafView {
    val startState = CreatedByViewManager(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case NewState(_, s) => s should contain(WriteTransformationTimestamp(viewUnderTest, 10))
      case _              => fail()
    }
  }

  it should "report materialization to issuer" in new NoOpLeafView {
    val startState = CreatedByViewManager(viewUnderTest)
    
    viewUnderTest.materialize(startState, dependentView, successFlagExists = true, currentTime = 10) match {
      case NewState(_, s) => s should contain(ReportMaterialized(viewUnderTest, Set(dependentView), 10, false, false))
      case _              => fail()
    }
  }
  
  "A NoOp leaf view in CreatedByViewManagerState with no _SUCCESS flag" should "transition to NoData upon materialize" in new NoOpLeafView {
    val startState = CreatedByViewManager(viewUnderTest)

    viewUnderTest.materialize(startState, dependentView, successFlagExists = false) match {
      case NewState(NoData(v, viewTransformationChecksum), _) => v shouldBe viewUnderTest
      case _                                  => fail()
    }
  }
  
  it should "report no data available to issuer" in new NoOpLeafView {
    val startState = CreatedByViewManager(viewUnderTest)
    
    viewUnderTest.materialize(startState, dependentView, successFlagExists = false, currentTime = 10) match {
      case NewState(_, s) => s should contain(ReportNoDataAvailable(viewUnderTest, Set(dependentView)))
      case _              => fail()
    }
  }
}