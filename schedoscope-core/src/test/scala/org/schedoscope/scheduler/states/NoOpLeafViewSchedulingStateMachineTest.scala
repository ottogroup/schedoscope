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
import test.eci.datahub.ProductBrand

class NoOpLeafViewSchedulingStateMachineTest extends FlatSpec with Matchers {

  trait SuccessFlag {
    val stateMachine = new NoOpViewSchedulingStateMachine(true)
  }
  
  trait NoSuccessFlag {
    val stateMachine = new NoOpViewSchedulingStateMachine(false)
  }
  
  trait NoOpLeafView {
    val dependentView = ProductBrand(p("EC0101"), p("2014"), p("01"), p("01"))
    val viewUnderTest = dependentView.brand()
    val viewTransformationChecksum = viewUnderTest.transformation().checksum
  }

  "A NoOp leaf view in CreatedByViewManager state with existing _SUCCESS flag" should "transition to Materialized upon materialize" in new NoOpLeafView with SuccessFlag {
    val startState = CreatedByViewManager(viewUnderTest)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(Materialized(v, `viewTransformationChecksum`, 10, false, false), _) => v shouldBe viewUnderTest
      case _ => fail()
    }
  }

  it should "request writing of new transformation checksum" in new NoOpLeafView with SuccessFlag {
    val startState = CreatedByViewManager(viewUnderTest)

    stateMachine.materialize(startState, dependentView) match {
      case ResultingViewSchedulingState(_, s) => s should contain(WriteTransformationCheckum(viewUnderTest))
      case _                                  => fail()
    }
  }

  it should "request writing of new transformation time stamp" in new NoOpLeafView with SuccessFlag {
    val startState = CreatedByViewManager(viewUnderTest)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(_, s) => s should contain(WriteTransformationTimestamp(viewUnderTest, 10))
      case _                                  => fail()
    }
  }

  it should "report materialization to issuer" in new NoOpLeafView with SuccessFlag {
    val startState = CreatedByViewManager(viewUnderTest)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(_, s) => s should contain(ReportMaterialized(viewUnderTest, Set(dependentView), 10, withErrors = false, incomplete = false))
      case _                                  => fail()
    }
  }

  "A NoOp leaf view in CreatedByViewManager state with no _SUCCESS flag" should "transition to NoData upon materialize" in new NoOpLeafView with NoSuccessFlag {
    val startState = CreatedByViewManager(viewUnderTest)

    stateMachine.materialize(startState, dependentView) match {
      case ResultingViewSchedulingState(NoData(v), _) => v shouldBe viewUnderTest
      case _ => fail()
    }
  }

  it should "report no data available to issuer" in new NoOpLeafView with NoSuccessFlag {
    val startState = CreatedByViewManager(viewUnderTest)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(_, s) => s should contain(ReportNoDataAvailable(viewUnderTest, Set(dependentView)))
      case _                                  => fail()
    }
  }

  "A NoOp leaf view in Materialized state with existing _SUCCESS flag" should "remain Materialized upon materialize" in new NoOpLeafView with SuccessFlag  {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10, withErrors = false, incomplete = false)

    stateMachine.materialize(startState, dependentView, currentTime = 20) match {
      case ResultingViewSchedulingState(Materialized(v, `viewTransformationChecksum`, 10, false, false), _) => v shouldBe viewUnderTest
      case _ => fail()
    }
  }

  it should "report materialization to issuer" in new NoOpLeafView with SuccessFlag   {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10, withErrors = false, incomplete = false)

    stateMachine.materialize(startState, dependentView, currentTime = 20) match {
      case ResultingViewSchedulingState(_, s) => s should contain(ReportMaterialized(viewUnderTest, Set(dependentView), 10, withErrors = false, incomplete = false))
      case _                                  => fail()
    }
  }
 
  it should "request writing of new transformation checksum if materialization mode is RESET_TRANSFORMATION_CHECKSUMS" in new NoOpLeafView with SuccessFlag {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10, withErrors = false, incomplete = false)

    stateMachine.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS) match {
      case ResultingViewSchedulingState(_, s) => s should contain(WriteTransformationCheckum(viewUnderTest))
      case _                                  => fail()
    }
  }

  "A NoOp leaf view in Materialized state without _SUCCESS flag" should "remain Materialized upon materialize" in new NoOpLeafView with NoSuccessFlag {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10, withErrors = false, incomplete = false)

    stateMachine.materialize(startState, dependentView, currentTime = 20) match {
      case ResultingViewSchedulingState(Materialized(v, `viewTransformationChecksum`, 10, false, false), _) => v shouldBe viewUnderTest
      case _ => fail()
    }
  }

  it should "report materialization to issuer" in new NoOpLeafView with NoSuccessFlag {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10, withErrors = false, incomplete = false)

    stateMachine.materialize(startState, dependentView, currentTime = 20) match {
      case ResultingViewSchedulingState(_, s) => s should contain(ReportMaterialized(viewUnderTest, Set(dependentView), 10, withErrors = false, incomplete = false))
      case _                                  => fail()
    }
  }

  it should "request writing of new transformation checksum if materialization mode is RESET_TRANSFORMATION_CHECKSUMS" in new NoOpLeafView with NoSuccessFlag {
    val startState = Materialized(viewUnderTest, viewTransformationChecksum, 10, withErrors = false, incomplete = false)

    stateMachine.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS) match {
      case ResultingViewSchedulingState(_, s) => s should contain(WriteTransformationCheckum(viewUnderTest))
      case _                                  => fail()
    }
  }

  "A NoOp leaf view in NoData state with existing _SUCCESS flag" should "transition to Materialized upon materialize" in new NoOpLeafView with SuccessFlag  {
    val startState = NoData(viewUnderTest)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(Materialized(v, `viewTransformationChecksum`, 10, false, false), _) => v shouldBe viewUnderTest
      case _ => fail()
    }
  }

  it should "request writing of new transformation checksum" in new NoOpLeafView with SuccessFlag {
    val startState = NoData(viewUnderTest)

    stateMachine.materialize(startState, dependentView) match {
      case ResultingViewSchedulingState(_, s) => s should contain(WriteTransformationCheckum(viewUnderTest))
      case _                                  => fail()
    }
  }

  it should "request writing of new transformation time stamp" in new NoOpLeafView with SuccessFlag {
    val startState = NoData(viewUnderTest)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(_, s) => s should contain(WriteTransformationTimestamp(viewUnderTest, 10))
      case _                                  => fail()
    }
  }

  it should "report materialization to issuer" in new NoOpLeafView with SuccessFlag {
    val startState = NoData(viewUnderTest)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(_, s) => s should contain(ReportMaterialized(viewUnderTest, Set(dependentView), 10, withErrors = false, incomplete = false))
      case _                                  => fail()
    }
  }

  "A NoOp leaf view in NoData state with no _SUCCESS flag" should "stay in NoData upon materialize" in new NoOpLeafView with NoSuccessFlag {
    val startState = NoData(viewUnderTest)

    stateMachine.materialize(startState, dependentView) match {
      case ResultingViewSchedulingState(NoData(v), _) => v shouldBe viewUnderTest
      case _ => fail()
    }
  }

  it should "report no data available to issuer" in new NoOpLeafView with NoSuccessFlag {
    val startState = NoData(viewUnderTest)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(_, s) => s should contain(ReportNoDataAvailable(viewUnderTest, Set(dependentView)))
      case _                                  => fail()
    }
  }

  "A NoOp leaf view in ReadFromSchemaManager state with existing _SUCCESS flag" should "transition to Materialized upon materialize" in new NoOpLeafView with SuccessFlag {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    stateMachine.materialize(startState, dependentView, currentTime = 20) match {
      case ResultingViewSchedulingState(Materialized(v, `viewTransformationChecksum`, 10, false, false), _) => v shouldBe viewUnderTest
      case _ => fail()
    }
  }

  it should "report materialization to issuer" in new NoOpLeafView with SuccessFlag {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    stateMachine.materialize(startState, dependentView, currentTime = 20) match {
      case ResultingViewSchedulingState(_, s) => s should contain(ReportMaterialized(viewUnderTest, Set(dependentView), 10, withErrors = false, incomplete = false))
      case _                                  => fail()
    }
  }

  it should "request writing of new transformation checksum if materialization mode is RESET_TRANSFORMATION_CHECKSUMS" in new NoOpLeafView with SuccessFlag {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    stateMachine.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS) match {
      case ResultingViewSchedulingState(_, s) => s should contain(WriteTransformationCheckum(viewUnderTest))
      case _                                  => fail()
    }
  }

  "A NoOp leaf view in ReadFromSchemaManager state with no _SUCCESS flag" should "transition to Materialized upon materialize" in new NoOpLeafView with NoSuccessFlag  {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    stateMachine.materialize(startState, dependentView, currentTime = 20) match {
      case ResultingViewSchedulingState(Materialized(v, `viewTransformationChecksum`, 10, false, false), _) => v shouldBe viewUnderTest
      case _ => fail()
    }
  }

  it should "report materialization to issuer" in new NoOpLeafView with NoSuccessFlag {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    stateMachine.materialize(startState, dependentView, currentTime = 20) match {
      case ResultingViewSchedulingState(_, s) => s should contain(ReportMaterialized(viewUnderTest, Set(dependentView), 10, withErrors = false, incomplete = false))
      case _                                  => fail()
    }
  }

  it should "request writing of new transformation checksum if materialization mode is RESET_TRANSFORMATION_CHECKSUMS" in new NoOpLeafView with NoSuccessFlag {
    val startState = ReadFromSchemaManager(viewUnderTest, viewTransformationChecksum, 10)

    stateMachine.materialize(startState, dependentView, materializationMode = RESET_TRANSFORMATION_CHECKSUMS) match {
      case ResultingViewSchedulingState(_, s) => s should contain(WriteTransformationCheckum(viewUnderTest))
      case _                                  => fail()
    }
  }

  "A NoOp leaf view in Invalidated state with existing _SUCCESS flag" should "transition to Materialized upon materialize" in new NoOpLeafView with SuccessFlag {
    val startState = Invalidated(viewUnderTest)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(Materialized(v, `viewTransformationChecksum`, 10, false, false), _) => v shouldBe viewUnderTest
      case _ => fail()
    }
  }

  it should "request writing of new transformation checksum" in new NoOpLeafView with SuccessFlag {
    val startState = Invalidated(viewUnderTest)

    stateMachine.materialize(startState, dependentView) match {
      case ResultingViewSchedulingState(_, s) => s should contain(WriteTransformationCheckum(viewUnderTest))
      case _                                  => fail()
    }
  }

  it should "request writing of new transformation time stamp" in new NoOpLeafView with SuccessFlag {
    val startState = Invalidated(viewUnderTest)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(_, s) => s should contain(WriteTransformationTimestamp(viewUnderTest, 10))
      case _                                  => fail()
    }
  }

  it should "report materialization to issuer" in new NoOpLeafView with SuccessFlag {
    val startState = Invalidated(viewUnderTest)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(_, s) => s should contain(ReportMaterialized(viewUnderTest, Set(dependentView), 10, withErrors = false, incomplete = false))
      case _                                  => fail()
    }
  }

  "A NoOp leaf view in Invalidated state with no _SUCCESS flag" should "transition to NoData upon materialize" in new NoOpLeafView with NoSuccessFlag {
    val startState = Invalidated(viewUnderTest)

    stateMachine.materialize(startState, dependentView) match {
      case ResultingViewSchedulingState(NoData(v), _) => v shouldBe viewUnderTest
      case _ => fail()
    }
  }

  it should "report no data available to issuer" in new NoOpLeafView with NoSuccessFlag {
    val startState = Invalidated(viewUnderTest)

    stateMachine.materialize(startState, dependentView, currentTime = 10) match {
      case ResultingViewSchedulingState(_, s) => s should contain(ReportNoDataAvailable(viewUnderTest, Set(dependentView)))
      case _                                  => fail()
    }
  }
}