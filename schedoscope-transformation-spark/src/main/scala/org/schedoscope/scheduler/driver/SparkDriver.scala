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

import java.io.File

import org.apache.spark.launcher.SparkAppHandle.State._
import org.apache.spark.launcher.SparkLauncher.{DRIVER_EXTRA_CLASSPATH, EXECUTOR_EXTRA_CLASSPATH}
import org.apache.spark.launcher.{ExitCodeAwareChildProcAppHandle, SparkAppHandle, SparkSubmitLauncher}
import org.joda.time.LocalDateTime
import org.schedoscope.Settings
import org.schedoscope.conf.DriverSettings
import org.schedoscope.dsl.transformations.SparkTransformation
import org.schedoscope.test.resources.TestResources
import org.slf4j.LoggerFactory

/**
  * This driver runs Spark transformations
  */
class SparkDriver(val driverRunCompletionHandlerClassNames: List[String]) extends DriverOnNonBlockingApi[SparkTransformation] {

  val log = LoggerFactory.getLogger(classOf[SparkDriver])

  override def transformationName = "spark"

  override def run(t: SparkTransformation): DriverRunHandle[SparkTransformation] = try {
    val l = new SparkSubmitLauncher()

    t match {

      case SparkTransformation(
      applicationName, mainJarOrPy, mainClass,
      applicationArgs,
      master, deployMode,
      additionalJars,
      additionalPys,
      additionalFiles,
      propertiesFile
      ) =>
        l.setAppName(applicationName)
        l.setAppResource(mainJarOrPy)

        if (mainClass != null)
          l.setMainClass(mainClass)

        l.addAppArgs(applicationArgs: _*)

        l.setMaster(master)

        //set name for the job
        l.setConf("spark.app.name",t.getViewUrl())

        if (master.startsWith("local"))
          l.setDeployMode("client")
        else
          l.setDeployMode(deployMode)

        additionalJars.foreach(l.addJar)

        additionalPys.foreach(l.addPyFile)

        additionalFiles.foreach(l.addFile)

        if (propertiesFile != null)
          l.setPropertiesFile(propertiesFile)

        t.configuration.foreach {
          case (k, v) =>
            if (k.startsWith("--"))
              l.addSparkArg(k, v.toString)
            else if (k.startsWith("spark."))
              l.setConf(k, v.toString)
            else
              l.setChildEnv(k, v.toString)
        }

        if (!master.startsWith("local")) {

          l.setConf(DRIVER_EXTRA_CLASSPATH, Settings().getDriverSettings(t).libDirectory + (
            if (l.getConf(DRIVER_EXTRA_CLASSPATH) != null)
              File.pathSeparator + l.getConf(DRIVER_EXTRA_CLASSPATH)
            else
              ""
            )
          )

          l.setConf(EXECUTOR_EXTRA_CLASSPATH, Settings().getDriverSettings(t).libDirectory + (
            if (l.getConf(EXECUTOR_EXTRA_CLASSPATH) != null)
              File.pathSeparator + l.getConf(EXECUTOR_EXTRA_CLASSPATH)
            else
              ""
            )
          )

        } else
          l.setLocalTestMode()

    }

    new DriverRunHandle[SparkTransformation](this, new LocalDateTime(), t, l.startApplication())
  } catch {
    case t: Throwable => throw new RetryableDriverException("Could not start Spark submit process because of exception", t)
  }


  override def getDriverRunState(run: DriverRunHandle[SparkTransformation]): DriverRunState[SparkTransformation] = {

    val appHandle = run.stateHandle.asInstanceOf[ExitCodeAwareChildProcAppHandle]
    val appInfo = run.transformation match {
      case SparkTransformation(
      applicationName, mainJarOrPy, mainClass, applicationArgs,
      _, _, _, _, _, _) =>
        s"$applicationName :: $mainJarOrPy ${if (mainClass != null) s" :: $mainClass" else ""} :: $applicationArgs"
    }

    try {
      val handleState = appHandle.getState
      val exitCode = appHandle.getExitCode

      log.debug(s"checking Spark app handle for $appInfo - state: $handleState exit code: $exitCode")

      (handleState, exitCode) match {

        case (KILLED, _) | (FAILED, _) => DriverRunFailed(this, s"Driver run for Spark transformation $appInfo failed with State $handleState (Exit Code $exitCode)", null)

        case (_, Some(e)) =>

          if (e > 0)
            DriverRunFailed(this, s"Driver run for Spark transformation $appInfo failed with problematic Exit Code $exitCode (State is $handleState)", null)
          else
            DriverRunSucceeded(this, s"Driver run for Spark transformation $appInfo completed with successful Exit Code $exitCode (State is $handleState)")

        case _ => DriverRunOngoing(this, run)

      }

    } catch {
      case t: Throwable => DriverRunFailed[SparkTransformation](this, s"Spark driver run failed with Exception for $appInfo", t)
    }
  }

  override def killRun(run: DriverRunHandle[SparkTransformation]): Unit = try {
    run.stateHandle.asInstanceOf[SparkAppHandle].kill()
  } catch {
    case t: Throwable => log.error("Spark driver failed to kill driver run", t)
  }


  /**
    * Rig Spark transformation for test by setting master to local and deploy mode to client.
    */
  override def rigTransformationForTest(t: SparkTransformation, testResources: TestResources) = t match {
    case SparkTransformation(
    applicationName, mainJarOrPy, mainClass,
    applicationArgs,
    _, _,
    additionalJars,
    additionalPys,
    additionalFiles,
    propertiesFile
    ) => SparkTransformation(
      applicationName, mainJarOrPy, mainClass,
      applicationArgs,
      "local", "client",
      additionalJars,
      additionalPys,
      additionalFiles,
      propertiesFile
    ).configureWith(t.configuration.toMap).asInstanceOf[SparkTransformation]
  }

}

object SparkDriver extends DriverCompanionObject[SparkTransformation] {

  def apply(ds: DriverSettings) = {
    new SparkDriver(ds.driverRunCompletionHandlers)
  }

  def apply(driverSettings: DriverSettings, testResources: TestResources): Driver[SparkTransformation] =
    new SparkDriver(List("org.schedoscope.test.resources.TestDriverRunCompletionHandler"))
}