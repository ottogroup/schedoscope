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

import net.lingala.zip4j.core.ZipFile
import org.apache.commons.io.FileUtils
import org.schedoscope.{ DriverSettings, Schedoscope }
import org.schedoscope.dsl.transformations.Transformation

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration.Duration
import scala.util.Random

/**
 * In Schedoscope, drivers are responsible for executing transformations.
 *
 * Drivers encapsulate the respective APIs required to perform a transformation type.
 * They might be executed from within the DriverActor or directly from a test.
 *
 * A driver is parameterized by the type of transformation that it is able to execute.
 *
 * An execution of a transformation - a driver run - is represented by a driver run handle. Depending on the
 * state of transformation execution, the current driver run state can be queried for a driver
 * run handle.
 */
trait Driver[T <: Transformation] {

  /**
   * The name of the transformations executed by this driver. Must be equal to t.name for any t: T.
   */
  def transformationName: String

  /**
   * Kill the given driver run. Default: do nothing
   */
  def killRun(run: DriverRunHandle[T]): Unit = {}

  /**
   * Get the current driver run state for a given driver run represented by the handle.
   */
  def getDriverRunState(run: DriverRunHandle[T]): DriverRunState[T]

  /**
   * Create a driver run, i.e., start the execution of the transformation asychronously.
   */
  def run(t: T): DriverRunHandle[T]

  /**
   * Execute the transformation synchronously and block until it's done. Return the final state
   * of the driver run.
   */
  def runAndWait(t: T): DriverRunState[T]

  /**
   * Deploy all resources for this transformation type to the cluster. By default, this deploys all
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
   *
   * E.g., provide a val of the same name to the constructor of the driver implementation.
   */
  def driverRunCompletionHandlerClassNames: List[String]

  lazy val driverRunCompletionHandlers: List[DriverRunCompletionHandler[T]] =
    driverRunCompletionHandlerClassNames.map { className => Class.forName(className).newInstance().asInstanceOf[DriverRunCompletionHandler[T]] }

  /**
   * Invokes completion handlers prior to the given driver run.
   */
  def driverRunStarted(run: DriverRunHandle[T]) {
    driverRunCompletionHandlers.foreach(_.driverRunStarted(run))
  }

  /**
   * Invokes completion handlers after the given driver run.
   */
  def driverRunCompleted(run: DriverRunHandle[T]) {
    getDriverRunState(run) match {
      case s: DriverRunSucceeded[T] => driverRunCompletionHandlers.foreach(_.driverRunCompleted(s, run))
      case f: DriverRunFailed[T]    => driverRunCompletionHandlers.foreach(_.driverRunCompleted(f, run))
      case _                        => throw RetryableDriverException("driverRunCompleted called with non-final driver run state")
    }
  }
}

/**
 * DriverOnBlockingApi provides a default implementation for most of the driver contract
 * for transformations working on blocking APIs.
 *
 * The asynchronism of the driver contract is implemented using futures. I.e., the state
 * handle of the respective driver run state is a future returning the final driver run state
 * for the transformation being executed.
 *
 * Subclasses only need to provide an implementation of the methods transformationName and run
 * as well as driverRunCompletionHandlerClassNames.
 *
 * As examples, @see HiveDriver, @see PigDriver, @see FileSystemDriver
 *
 */
trait DriverOnBlockingApi[T <: Transformation] extends Driver[T] {

  implicit val executionContext = Schedoscope.actorSystem.dispatchers.lookup("akka.actor.future-driver-dispatcher")

  def runTimeOut: Duration = Schedoscope.settings.getDriverSettings(transformationName).timeout

  def getDriverRunState(run: DriverRunHandle[T]): DriverRunState[T] = {
    val runState = run.stateHandle.asInstanceOf[Future[DriverRunState[T]]]
    if (runState.isCompleted)
      runState.value.get.get
    else
      DriverRunOngoing[T](this, run)
  }

  def runAndWait(t: T): DriverRunState[T] = Await.result(run(t).stateHandle.asInstanceOf[Future[DriverRunState[T]]], runTimeOut)
}

/**
 * DriverOnNonBlockingApi provides a simple default implementation for parts of the driver
 * contract for asynchronous APIs (namely, the runAndWait method).
 *
 * The state handle of driver run handles for such APIs should be the corresponding handle
 * mechanism used by that API.
 *
 * As examples, @see MapreduceDriver and @see OozieDriver
 */
trait DriverOnNonBlockingApi[T <: Transformation] extends Driver[T] {

  def runAndWait(t: T): DriverRunState[T] = {
    val runHandle = run(t)

    while (getDriverRunState(runHandle).isInstanceOf[DriverRunOngoing[T]])
      Thread.sleep(5000)

    getDriverRunState(runHandle)
  }

}

/**
 * Companion object with factory methods for drivers
 */
object Driver {
  /**
   * Returns the names of the transformations for which drivers are configured.
   */
  def transformationsWithDrivers = Schedoscope.settings.availableTransformations.keySet()

  /**
   * Returns the driver settings for a given transformation type.
   */
  def driverSettings(transformationName: String): DriverSettings = Schedoscope.settings.getDriverSettings(transformationName)

  /**
   * Returns the driver settings for a given transformation.
   */
  def driverSettings(t: Transformation): DriverSettings = driverSettings(t.name)

  /**
   * Returns an appropriately set up driver for the given transformation type and the given driver settings.
   */
  def driverFor(transformationName: String, ds: DriverSettings): Driver[Transformation] = (transformationName match {

    case "hive"       => HiveDriver(ds)

    case "mapreduce"  => MapreduceDriver(ds)

    case "pig"        => PigDriver(ds)

    case "filesystem" => FileSystemDriver(ds)

    case "oozie"      => OozieDriver(ds)

    case "shell"      => ShellDriver(ds)

    case "seq"        => SeqDriver(ds)

    case _            => throw new IllegalArgumentException(s"Driver for ${transformationName} transformation not found")

  }).asInstanceOf[Driver[Transformation]]

  /**
   * Returns an appropriately set up driver for the given transformation type using the configured settings.
   */
  def driverFor(transformationName: String): Driver[Transformation] = driverFor(transformationName, driverSettings(transformationName))

  /**
   * Returns an appropriately set up  driver for the given transformation and the given driver settings.
   */
  def driverFor(t: Transformation, ds: DriverSettings): Driver[Transformation] = driverFor(t.name, ds)

  /**
   * Returns an appropriately set up  driver for the given transformation and the configured settings.
   */
  def driverFor(t: Transformation): Driver[Transformation] = driverFor(t.name, driverSettings(t))

}
