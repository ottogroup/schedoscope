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
package org.schedoscope.scheduler.driver

import org.schedoscope.{ DriverSettings, Schedoscope }
import org.schedoscope.dsl.transformations.{ Transformation, SeqTransformation }
import org.joda.time.LocalDateTime

/**
 * Driver for executing Seq transformations. This is just a delegate to the drivers for
 * the transformation types the Seq is composed of.
 */
class SeqDriver[T1 <: Transformation, T2 <: Transformation](val driverRunCompletionHandlerClassNames: List[String], driverFor: (String) => Driver[_]) extends DriverOnNonBlockingApi[SeqTransformation[T1, T2]] {

  /**
   * Possible states capturing how far the Seq transformation has progressed.
   */
  sealed abstract class SeqDriverStateHandle

  /**
   * First transformation of Seq still not finished
   */
  case class FirstTransformationOngoing(firstRun: DriverRunHandle[T1]) extends SeqDriverStateHandle

  /**
   * First transformation has finished. This will be the final state if the first transformation has failed.
   * Otherwise, this state will be left as soon as the second transformation commences.
   */
  case class FirstTransformationFinished(firstRunState: DriverRunState[T1]) extends SeqDriverStateHandle

  /**
   * The first transformation has been run successfully. Now the second transformation is ongoing.
   */
  case class SecondTransformationOngoing(firstRunState: DriverRunState[T1], secondRun: DriverRunHandle[T2]) extends SeqDriverStateHandle

  /**
   * The first transformation has been run successfully. The second transformation has finished. It
   * may have failed or succeeded.
   */
  case class Finished(firstRunState: DriverRunState[T1], secondRunState: DriverRunState[T2]) extends SeqDriverStateHandle

  def transformationName = "seq"

  def run(t: SeqTransformation[T1, T2]): DriverRunHandle[SeqTransformation[T1, T2]] = {

    val firstTransformation = t.firstThisTransformation
    val driverForFirstTransformation = driverFor(firstTransformation.name).asInstanceOf[Driver[T1]]

    val firstTransformationRunHandle = driverForFirstTransformation.run(firstTransformation)

    new DriverRunHandle[SeqTransformation[T1, T2]](this, new LocalDateTime(), t, FirstTransformationOngoing(firstTransformationRunHandle))
  }

  def getDriverRunState(run: DriverRunHandle[SeqTransformation[T1, T2]]): DriverRunState[SeqTransformation[T1, T2]] = {

    val driverForFirstTransformation = driverFor(run.transformation.firstThisTransformation.name).asInstanceOf[Driver[T1]]
    val driverForSecondTransformation = driverFor(run.transformation.thenThatTransformation.name).asInstanceOf[Driver[T2]]

    run.stateHandle.asInstanceOf[SeqDriverStateHandle] match {

      case FirstTransformationOngoing(firstRunHandle) => {
        val firstTransformationState = driverForFirstTransformation.getDriverRunState(firstRunHandle)

        firstTransformationState match {

          case s: DriverRunSucceeded[T1] => {
            run.stateHandle = FirstTransformationFinished(firstTransformationState)
            DriverRunOngoing(this, run)
          }

          case f: DriverRunFailed[T1] => {
            run.stateHandle = FirstTransformationFinished(firstTransformationState)
            DriverRunFailed(this, s"First transformation in SeqTransformation failed: ${f.reason}", f.cause)
          }

          case o: DriverRunOngoing[T1] =>
            DriverRunOngoing(this, run)

        }

      }

      case FirstTransformationFinished(firstRunState) => firstRunState match {

        case s: DriverRunSucceeded[T1] => {
          val secondTransformationRunHandle = driverForSecondTransformation.run(run.transformation.thenThatTransformation)
          run.stateHandle = SecondTransformationOngoing(s, secondTransformationRunHandle)
          DriverRunOngoing(this, run)
        }

        case f: DriverRunFailed[T1] =>
          DriverRunFailed(this, s"First transformation in Seq transformation failed: ${f.reason}", f.cause)

        case o: DriverRunOngoing[T1] => throw new RuntimeException("Seq transformation should never have finished first transformation with DriverRunOngoing state")

      }

      case SecondTransformationOngoing(firstRunState, secondRunHandle) => {
        val secondTransformationState = driverForSecondTransformation.getDriverRunState(secondRunHandle)

        secondTransformationState match {

          case s: DriverRunSucceeded[T2] => {
            run.stateHandle = Finished(firstRunState, secondTransformationState)
            DriverRunSucceeded(this, s"Seq transformation executed: ${run.transformation}")
          }

          case f: DriverRunFailed[T2] => {
            run.stateHandle = Finished(firstRunState, secondTransformationState)

            if (run.transformation.firstTransformationIsDriving)
              DriverRunSucceeded(this, s"Seq transformation executed: ${run.transformation} with ignored failure of second transformation: ${f.reason}, cause: ${f.cause}")
            else
              DriverRunFailed(this, s"Second transformation in SeqTransformation failed: ${f.reason}", f.cause)
          }

          case o: DriverRunOngoing[T2] =>
            DriverRunOngoing(this, run)

        }
      }

      case Finished(firstRunState, secondRunState) => secondRunState match {

        case f: DriverRunFailed[T2] =>
          if (run.transformation.firstTransformationIsDriving)
            DriverRunSucceeded(this, s"Seq transformation executed: ${run.transformation} with ignored failure of second transformation: ${f.reason}, cause: ${f.cause}")
          else
            DriverRunFailed(this, s"Second transformation in SeqTransformation failed: ${f.reason}", f.cause)

        case s: DriverRunSucceeded[T2] =>
          DriverRunSucceeded(this, s"Seq transformation executed: ${run.transformation}")

        case o: DriverRunOngoing[T2] => throw new RuntimeException("Seq transformation should never have finished second transformation with DriverRunOngoing state")

      }
    }
  }

  override def killRun(run: DriverRunHandle[SeqTransformation[T1, T2]]): Unit = run.stateHandle.asInstanceOf[SeqDriverStateHandle] match {

    case FirstTransformationOngoing(firstRunHandle) => {
      val driverForFirstTransformation = driverFor(run.transformation.firstThisTransformation.name).asInstanceOf[Driver[T1]]

      driverForFirstTransformation.killRun(firstRunHandle)
    }

    case SecondTransformationOngoing(_, secondRunHandle) => {
      val driverForSecondTransformation = driverFor(run.transformation.thenThatTransformation.name).asInstanceOf[Driver[T2]]

      driverForSecondTransformation.killRun(secondRunHandle)
    }

    case _ =>
  }
}

/**
 * Factory for Seq driver
 */
object SeqDriver {
  def apply[T1 <: Transformation, T2 <: Transformation](ds: DriverSettings) =
    new SeqDriver[T1, T2](ds.driverRunCompletionHandlers, (transformationName: String) => Driver.driverFor(transformationName, ds))
}