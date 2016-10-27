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

import org.joda.time.LocalDateTime
import org.schedoscope.conf.DriverSettings
import org.schedoscope.dsl.transformations.{SeqTransformation, Transformation}
import org.schedoscope.test.resources.TestResources
import org.slf4j.LoggerFactory

import scala.language.existentials

/**
  * Driver for executing Seq transformations. This is just a delegate to the drivers for
  * the transformation types the Seq is composed of.
  */
class SeqDriver(val driverRunCompletionHandlerClassNames: List[String], driverFor: (String) => Driver[Transformation]) extends DriverOnNonBlockingApi[SeqTransformation[Transformation, Transformation]] {

  val log = LoggerFactory.getLogger(classOf[SeqDriver])

  /**
    * Possible states capturing how far the Seq transformation has progressed.
    */
  sealed abstract class SeqDriverStateHandle

  /**
    * First transformation of Seq still not finished
    */
  case class FirstTransformationOngoing(firstRun: DriverRunHandle[Transformation]) extends SeqDriverStateHandle

  /**
    * First transformation has finished. This will be the final state if the first transformation has failed.
    * Otherwise, this state will be left as soon as the second transformation commences.
    */
  case class FirstTransformationFinished(firstRunState: DriverRunState[Transformation]) extends SeqDriverStateHandle

  /**
    * The first transformation has been run successfully. Now the second transformation is ongoing.
    */
  case class SecondTransformationOngoing(firstRunState: DriverRunState[Transformation], secondRun: DriverRunHandle[Transformation]) extends SeqDriverStateHandle

  /**
    * The first transformation has been run successfully. The second transformation has finished. It
    * may have failed or succeeded.
    */
  case class Finished(firstRunState: DriverRunState[Transformation], secondRunState: DriverRunState[Transformation]) extends SeqDriverStateHandle

  def transformationName = "seq"

  def run(t: SeqTransformation[Transformation, Transformation]): DriverRunHandle[SeqTransformation[Transformation, Transformation]] = {

    val firstTransformation = t.firstThisTransformation
    val driverForFirstTransformation = driverFor(firstTransformation.name)

    log.info(s"SeqDriver running first transformation $firstTransformation with driver $driverForFirstTransformation")

    val firstTransformationRunHandle = driverForFirstTransformation.run(firstTransformation)

    new DriverRunHandle[SeqTransformation[Transformation, Transformation]](this, new LocalDateTime(), t, FirstTransformationOngoing(firstTransformationRunHandle))
  }

  def getDriverRunState(run: DriverRunHandle[SeqTransformation[Transformation, Transformation]]): DriverRunState[SeqTransformation[Transformation, Transformation]] = {

    val driverForFirstTransformation = driverFor(run.transformation.firstThisTransformation.name)
    val driverForSecondTransformation = driverFor(run.transformation.thenThatTransformation.name)



    run.stateHandle.asInstanceOf[SeqDriverStateHandle] match {

      case FirstTransformationOngoing(firstRunHandle) => {
        val firstTransformationState = driverForFirstTransformation.getDriverRunState(firstRunHandle)

        firstTransformationState match {

          case s: DriverRunSucceeded[Transformation] => {
            run.stateHandle = FirstTransformationFinished(firstTransformationState)
            DriverRunOngoing(this, run)
          }

          case f: DriverRunFailed[Transformation] => {
            run.stateHandle = FirstTransformationFinished(firstTransformationState)

            log.error(s"First transformation ${run.transformation.firstThisTransformation} with SeqDriver $driverForFirstTransformation failed with reason ${f.reason} and cause ${f.cause}", f.cause)

            DriverRunFailed(this, s"First transformation in SeqTransformation failed: ${f.reason}", f.cause)
          }

          case o: DriverRunOngoing[Transformation] =>
            DriverRunOngoing(this, run)

        }

      }

      case FirstTransformationFinished(firstRunState) => firstRunState match {

        case s: DriverRunSucceeded[Transformation] => {

          log.info(s"SeqDriver handing over to driver $driverForSecondTransformation for second transformation ${run.transformation.thenThatTransformation}")

          val secondTransformationRunHandle = driverForSecondTransformation.run(run.transformation.thenThatTransformation)
          run.stateHandle = SecondTransformationOngoing(s, secondTransformationRunHandle)


          DriverRunOngoing(this, run)
        }

        case f: DriverRunFailed[Transformation] =>
          log.error(s"First transformation ${run.transformation.firstThisTransformation} with SeqDriver $driverForFirstTransformation failed with reason ${f.reason} and cause ${f.cause}", f.cause)

          DriverRunFailed(this, s"First transformation in Seq transformation failed: ${f.reason}", f.cause)

        case o: DriverRunOngoing[Transformation] => throw new RuntimeException("Seq transformation should never have finished first transformation with DriverRunOngoing state")

      }

      case SecondTransformationOngoing(firstRunState, secondRunHandle) => {
        val secondTransformationState = driverForSecondTransformation.getDriverRunState(secondRunHandle)

        secondTransformationState match {

          case s: DriverRunSucceeded[Transformation] => {
            run.stateHandle = Finished(firstRunState, secondTransformationState)
            DriverRunSucceeded(this, s"Seq transformation executed: ${run.transformation}")
          }

          case f: DriverRunFailed[Transformation] => {
            run.stateHandle = Finished(firstRunState, secondTransformationState)

            if (run.transformation.firstTransformationIsDriving) {

              log.warn(s"Second transformation ${run.transformation.thenThatTransformation} with SeqDriver $driverForSecondTransformation failed with reason ${f.reason} and cause ${f.cause}", f.cause)

              DriverRunSucceeded(this, s"Seq transformation executed: ${run.transformation} with ignored failure of second transformation: ${f.reason}, cause: ${f.cause}")
            } else {

              log.error(s"Second transformation ${run.transformation.thenThatTransformation} with SeqDriver $driverForSecondTransformation failed with reason ${f.reason} and cause ${f.cause}", f.cause)

              DriverRunFailed(this, s"Second transformation in SeqTransformation failed: ${f.reason}", f.cause)
            }
          }

          case o: DriverRunOngoing[Transformation] =>
            DriverRunOngoing(this, run)

        }
      }

      case Finished(firstRunState, secondRunState) => secondRunState match {

        case f: DriverRunFailed[Transformation] =>
          if (run.transformation.firstTransformationIsDriving)
            DriverRunSucceeded(this, s"Seq transformation executed: ${run.transformation} with ignored failure of second transformation: ${f.reason}, cause: ${f.cause}")
          else
            DriverRunFailed(this, s"Second transformation in SeqTransformation failed: ${f.reason}", f.cause)

        case s: DriverRunSucceeded[Transformation] =>
          DriverRunSucceeded(this, s"Seq transformation executed: ${run.transformation}")

        case o: DriverRunOngoing[Transformation] => throw new RuntimeException("Seq transformation should never have finished second transformation with DriverRunOngoing state")

      }
    }
  }

  override def killRun(run: DriverRunHandle[SeqTransformation[Transformation, Transformation]]): Unit = run.stateHandle.asInstanceOf[SeqDriverStateHandle] match {

    case FirstTransformationOngoing(firstRunHandle) => {
      val driverForFirstTransformation = driverFor(run.transformation.firstThisTransformation.name)

      driverForFirstTransformation.killRun(firstRunHandle)
    }

    case SecondTransformationOngoing(_, secondRunHandle) => {
      val driverForSecondTransformation = driverFor(run.transformation.thenThatTransformation.name)

      driverForSecondTransformation.killRun(secondRunHandle)
    }

    case _ =>
  }

  /**
    * Perform any rigging of a transformation necessary to execute it within the scope of the
    * test framework represented by an instance of TestResources using this driver.
    *
    * The rigged transformation is returned.
    *
    * By default, the transformation is not changed.
    */
  override def rigTransformationForTest(t: SeqTransformation[Transformation, Transformation],
                                        testResources: TestResources): SeqTransformation[Transformation, Transformation] = {
    val driverForFirstTransformation = driverFor(t.firstThisTransformation.name)
    val driverForSecondTransformation = driverFor(t.thenThatTransformation.name)

    val t1 = driverForFirstTransformation.rigTransformationForTest(t.firstThisTransformation, testResources)
    val t2 = driverForSecondTransformation.rigTransformationForTest(t.thenThatTransformation, testResources)

    SeqTransformation(t1, t2, t.firstTransformationIsDriving)
  }
}

/**
  * Factory for Seq driver
  */
object SeqDriver extends DriverCompanionObject[SeqTransformation[Transformation, Transformation]] {

  def apply(ds: DriverSettings) =
    new SeqDriver(ds.driverRunCompletionHandlers, (transformationName: String) => Driver.driverFor(transformationName))

  def apply(ds: DriverSettings, testResources: TestResources) =
    new SeqDriver(List("org.schedoscope.test.resources.TestDriverRunCompletionHandler"), (transformationName: String) => testResources.driverFor(transformationName))
}