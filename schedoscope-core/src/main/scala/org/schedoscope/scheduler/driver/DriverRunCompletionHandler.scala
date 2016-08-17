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

import org.schedoscope.dsl.transformations.Transformation

/**
  * Trait for user defined code to be executed before and after  a transformation. e.g. for gathering statistics
  * and logging information from the execution framework (e.g. mapreduce)
  *
  * As for exceptions:
  *
  * Should a failure within the completion handler not cause the transformation to fail, the exception should be suppressed.
  *
  * Should a failure within the completion handler cause a restart of the driver actor and a redo of the transformation,
  * it should be raised as a DriverException.
  *
  * Any other raised exception will cause the driver run to fail and the transformation subsequently to be retried by the view actor.
  *
  */
trait DriverRunCompletionHandler[T <: Transformation] {

  /**
    * This method is called immediately after a driver started a transformation.
    * This can be used to take measurements before the execution of a driver run
    * or other setup tasks.
    */
  def driverRunStarted(run: DriverRunHandle[T])

  /**
    * This method has to be implemented for gathering information about a completed run
    *
    */
  def driverRunCompleted(stateOfCompletion: DriverRunState[T], run: DriverRunHandle[T])
}

/**
  * Default implementation of a completion handler. Does nothing.
  */
class DoNothingCompletionHandler[T <: Transformation] extends DriverRunCompletionHandler[T] {

  def driverRunStarted(run: DriverRunHandle[T]) {}

  def driverRunCompleted(stateOfCompletion: DriverRunState[T], run: DriverRunHandle[T]) {}

}
