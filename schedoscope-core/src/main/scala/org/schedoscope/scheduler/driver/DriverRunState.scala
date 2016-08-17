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
  * Base class for driver run's state, contains reference to driver instance (e.g. to execute code for termination)
  */
sealed abstract class DriverRunState[T <: Transformation](val driver: Driver[T])

/**
  * Driver run state: transformation is still being executed
  */
case class DriverRunOngoing[T <: Transformation](override val driver: Driver[T], val runHandle: DriverRunHandle[T]) extends DriverRunState[T](driver)

/**
  * Driver run state: transformation has finished succesfully. The driver actor embedding the driver having sucessfully
  * executed the transformation will return a success message to the view actor initiating the transformation.
  */
case class DriverRunSucceeded[T <: Transformation](override val driver: Driver[T], comment: String) extends DriverRunState[T](driver)

/**
  * Driver run state: transformation has terminated with an error. The driver actor embedding the driver having failed
  * at executing the transformation will return a failure message to the view actor initiating the transformation. That view
  * actor will subsequently retry the transformation.
  *
  */
case class DriverRunFailed[T <: Transformation](override val driver: Driver[T], reason: String, cause: Throwable) extends DriverRunState[T](driver)

/**
  * Exceptions occurring in a driver that merit a retry. These will be escalated to the driver actor
  * to cause a driver actor restart.
  */
case class RetryableDriverException(message: String = null, cause: Throwable = null) extends RuntimeException(message, cause)

