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

import java.nio.file.Files
import scala.Array.canBuildFrom
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.Random
import org.joda.time.LocalDateTime
import org.schedoscope.DriverSettings
import org.schedoscope.dsl.transformations.Transformation
import net.lingala.zip4j.core.ZipFile
import org.apache.commons.io.FileUtils
import org.schedoscope.Schedoscope

/**
 *  Base class of exceptions that will be escalated to the driver actor  to cause a driver actor restart
 */
case class DriverException(message: String = null, cause: Throwable = null) extends RuntimeException(message, cause)

/**
 *  Handle for the transformation executed by a driver, called a driver run.
 */
class DriverRunHandle[T <: Transformation](val driver: Driver[T], val started: LocalDateTime, val transformation: T, val stateHandle: Any)

/**
 * Base class for driver run's state, contains reference to driver instance (e.g. to execute code for termination)
 *
 */
sealed abstract class DriverRunState[T <: Transformation](val driver: Driver[T])

/**
 *  Driver run state: transformation is still running
 */
case class DriverRunOngoing[T <: Transformation](override val driver: Driver[T], val runHandle: DriverRunHandle[T]) extends DriverRunState[T](driver)

/**
 *  Driver run state: transformation has finished succesfully. The driver actor embedding the driver having sucessfully
 *  executed the transformation will return a success message to the view actor initiating the transformation.
 */
case class DriverRunSucceeded[T <: Transformation](override val driver: Driver[T], comment: String) extends DriverRunState[T](driver)

/**
 *  Driver run state: transformation has terminated with an error. The driver actor embedding the driver having failed
 *  at executing the transformation will return a failure message to the view actor initiating the transformation. That view
 *  actor will subsequently retry the transformation.
 *
 */
case class DriverRunFailed[T <: Transformation](override val driver: Driver[T], reason: String, cause: Throwable) extends DriverRunState[T](driver)

/**
 * Trait for user defined code to be executed after a transformation. e.g. for gathering statistics
 * and logging information from the execution framework (e.g. mapreduce)
 */
trait DriverRunCompletionHandler[T <: Transformation] {

  /**
   * This method has to be implemented for gathering information about a completed run run
   *
   * As for exceptions:
   *
   * In case a failure within the completion handler should not cause the transformation to fail, the exception should be suppressed.
   *
   * In case a failure within the completion handler should cause a restart of the driver actor and a redo of the transformation,
   * it should be raised as a DriverException.
   *
   * Any other raised exception will cause the driver run to fail and the transformation subsequently to be retried by the view actor.
   */
  def driverRunCompleted(stateOfCompletion: DriverRunState[T], run: DriverRunHandle[T])
}

/**
 * Default implementation of a completion handler. Does nothing
 */
class DoNothingCompletionHandler[T <: Transformation] extends DriverRunCompletionHandler[T] {
  def driverRunCompleted(stateOfCompletion: DriverRunState[T], run: DriverRunHandle[T]) {}
}

/**
 * In Schedoscope, drivers are responsible for actually executing transformations. Drivers might be
 * executed from within the DriverActor or directly from a test. A
 * Driver is parameterized by the type of transformation that it is able to execute.
 *
 * Generally, there are two classes of implementations of this trait depending on the API
 * supporting a transformation. For blocking APIs, driver run states are implemented using futures.
 * For non-blocking APIs, driver run states can encapsulate whatever handler mechanism is supported
 * by the API.
 *
 * The present trait provides default implementations for blocking APIs. To support one, the methods
 * transformationName, run, and driverRunCompletionHandlerClassNames need to be overridden
 * (for example @see HiveDriver). Run needs to return an appropriate DriverRunHandle with a 
 * future as its stateHandle which produces a DriverRunState or throws an exception.
 *
 * For non-blocking APIs, one needs to override transformationName, killRun, getDriverRunState, run,
 * runAndWait, driverRunCompletionHandlerClassNames for the appropriate handle type of the API. As
 * an example @see OozieDriver.
 *
 */
trait Driver[T <: Transformation] {

  implicit val executionContext = Schedoscope.actorSystem.dispatchers.lookup("akka.actor.future-driver-dispatcher")

  /**
   * @return the name of the transformation. This is a string identifier of the transformation type
   * used within configurations.
   */
  def transformationName: String

  /**
   * A driver can override this to have a fixed timeout
   */
  def runTimeOut: Duration = Schedoscope.settings.getDriverSettings(transformationName).timeout

  /**
   * Kill the given driver run, default: do nothing
   */
  def killRun(run: DriverRunHandle[T]): Unit = {}

  /**
   * Retrieve the driver state from a run handle.
   */
  def getDriverRunState(run: DriverRunHandle[T]): DriverRunState[T] = {
    val runState = run.stateHandle.asInstanceOf[Future[DriverRunState[T]]]
    if (runState.isCompleted)
      runState.value.get.get
    else
      DriverRunOngoing[T](this, run)
  }

  /**
   * Run the transformation asychronously/nonblocking
   */
  def run(t: T): DriverRunHandle[T]

  /**
   * Run the transformation sychronously/blocking (e.g. for tests)
   */
  def runAndWait(t: T): DriverRunState[T] = Await.result(run(t).stateHandle.asInstanceOf[Future[DriverRunState[T]]], runTimeOut)

  /**
   * Deploy all resources for this transformation/view to the cluster. By default, deploys all
   * jars defined in the libJars section of the transformation configuration (@see DriverSettings)
   */
  def deployAll(ds: DriverSettings): Boolean = {
    val fsd = FileSystemDriver(ds)

    // clear destination
    fsd.delete(ds.location, true)
    fsd.mkdirs(ds.location)

    val succ = ds.libJars
      .map(f => {
        if (ds.unpack) {
          val tmpDir = Files.createTempDirectory("schedoscope-" + Random.nextLong.abs.toString).toFile
          new ZipFile(f.replaceAll("file:", "")).extractAll(tmpDir.getAbsolutePath)
          val succ = fsd.copy("file://" + tmpDir + "/*", ds.location, true)
          FileUtils.deleteDirectory(tmpDir)
          succ
        } else {
          fsd.copy(f, ds.location, true)
        }
      })

    succ.filter(_.isInstanceOf[DriverRunFailed[_]]).isEmpty
  }

  /**
   * Needs to be overridden to return the class names of driver run completion handlers to apply.
   * E.g., provide a val of the same name to the constructor of the driver implementation.
   */
  def driverRunCompletionHandlerClassNames: List[String]

  lazy val driverRunCompletionHandlers: List[DriverRunCompletionHandler[T]] =
    driverRunCompletionHandlerClassNames.map { className => Class.forName(className).newInstance().asInstanceOf[DriverRunCompletionHandler[T]] }

  /**
   * Invokes completion handler on the given driver run.
   */
  def driverRunCompleted(run: DriverRunHandle[T]) {
    getDriverRunState(run) match {
      case s: DriverRunSucceeded[T] => driverRunCompletionHandlers.foreach(_.driverRunCompleted(s, run))
      case f: DriverRunFailed[T]    => driverRunCompletionHandlers.foreach(_.driverRunCompleted(f, run))
      case _                        => throw DriverException("driverRunCompleted called with non-final driver run state")
    }
  }
}
