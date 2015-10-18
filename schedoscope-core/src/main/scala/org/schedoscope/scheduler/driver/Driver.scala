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
import org.schedoscope.dsl.Transformation
import net.lingala.zip4j.core.ZipFile
import org.schedoscope.Settings
import org.apache.commons.io.FileUtils

/**
 *  Encapsulation of exceptions that will be escalated to the DriverActor causing a restart
 */
case class DriverException(message: String = null, cause: Throwable = null) extends RuntimeException(message, cause)

/**
 *  Encapsulation of driver state. Also used to terminate a running action
 *
 * @param <T>
 */
class DriverRunHandle[T <: Transformation](val driver: Driver[T], val started: LocalDateTime, val transformation: T, val stateHandle: Any)

/**
 * Driver state, contains reference to driver instance (e.g. to execute code for termination)
 *
 * @param <T>
 */
sealed abstract class DriverRunState[T <: Transformation](val driver: Driver[T])

/**
 *  State: transformation is still running
 * @param <T>
 */
case class DriverRunOngoing[T <: Transformation](override val driver: Driver[T], val runHandle: DriverRunHandle[T]) extends DriverRunState[T](driver)

/**
 *  State: transformation has finished succesfully
 * @param <T>
 */
case class DriverRunSucceeded[T <: Transformation](override val driver: Driver[T], comment: String) extends DriverRunState[T](driver)
/**
 *  State: transformation has terminated with an error
 *
 * @param <T>
 */
case class DriverRunFailed[T <: Transformation](override val driver: Driver[T], reason: String, cause: Throwable) extends DriverRunState[T](driver)

/**
 * Trait for user defined code to be executed after a transformation. e.g. for gathering statistics
 * and logging information from the execution framework (e.g. mapreduce)
 * @param <T>
 */
trait DriverRunCompletionHandler[T <: Transformation] {

  /**
   * User has to implement this method for gathering information about a run
   * @param stateOfCompletion
   * @param run
   */
  def driverRunCompleted(stateOfCompletion: DriverRunState[T], run: DriverRunHandle[T])
}

/**
 * default implementation of a completion handler. does nothing
 * @param <T>
 */
class DoNothingCompletionHandler[T <: Transformation] extends DriverRunCompletionHandler[T] {
  def driverRunCompleted(stateOfCompletion: DriverRunState[T], run: DriverRunHandle[T]) {}
}

/**
 * In Schedoscope, drivers are responsible for actually executing transformations. Drivers might be
 * executed from within the DriverActor or directly from a Test or by the TestFramework. A
 * Driver is parametrized by the type of transformation that it is able to execute
 *
 * @param <T>  transformation type
 */
trait Driver[T <: Transformation] {

  /**
   * @return
   */
  def transformationName: String

  /**
   * A driver can override this to have a fixed timeout
   * @return
   */
  def runTimeOut: Duration = Settings().getDriverSettings(transformationName).timeout

  /**
   * Kill the currently running transformation process, default: do nothing
   * @param run
   */
  def killRun(run: DriverRunHandle[T]): Unit = {}

  /**
   * Retrieve the driver state from a runHandle.
   * @param run
   * @return
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
   *
   * @param t
   * @return
   */
  def run(t: T): DriverRunHandle[T]

  /**
   * Run the transformation sychronously/blocking (e.g. for tests)
   *
   * @param t
   * @return
   */
  def runAndWait(t: T): DriverRunState[T] = Await.result(run(t).stateHandle.asInstanceOf[Future[DriverRunState[T]]], runTimeOut)

  /**
   * Deploy all resources for this transformation/view to the cluster. By default, deploys all
   * jars defined in the libJars section of the transformation configuration (@see DriverSettings)
   *
   * @param ds
   * @return
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
  // completionHandlers to instanciate.
  def driverRunCompletionHandlerClassNames: List[String]
  // actual instances of the completion handlers
  lazy val driverRunCompletionHandlers: List[DriverRunCompletionHandler[T]] = try {
    driverRunCompletionHandlerClassNames.map { className => Class.forName(className).newInstance().asInstanceOf[DriverRunCompletionHandler[T]] }
  } catch {
    case t: Throwable => throw DriverException("Driver run completion handler could not be instantiated", t)
  }

  /**
   * Invokes CompletionHandlers depending on the result of the transformation
   * @param run
   */
  def driverRunCompleted(run: DriverRunHandle[T]) {
    getDriverRunState(run) match {
      case s: DriverRunSucceeded[T] => driverRunCompletionHandlers.foreach(_.driverRunCompleted(s, run))
      case f: DriverRunFailed[T]    => driverRunCompletionHandlers.foreach(_.driverRunCompleted(f, run))
      case _                        => throw DriverException("driverRunCompleted called with non-final driver run state")
    }
  }
}
