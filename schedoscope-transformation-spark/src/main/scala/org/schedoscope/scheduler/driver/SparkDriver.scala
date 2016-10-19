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

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.spark.launcher.SparkAppHandle.State._
import org.apache.spark.launcher.{ExitCodeAwareChildProcAppHandle, SparkAppHandle, SparkSubmitLauncher}
import org.joda.time.LocalDateTime
import org.schedoscope.Schedoscope
import org.schedoscope.conf.DriverSettings
import org.schedoscope.dsl.transformations.SparkTransformation
import org.schedoscope.test.resources.TestResources
import org.slf4j.LoggerFactory

/**
  * This driver runs Spark transformations
  */
class SparkDriver(val driverRunCompletionHandlerClassNames: List[String], val conf: HiveConf) extends DriverOnNonBlockingApi[SparkTransformation] {

  val log = LoggerFactory.getLogger(classOf[SparkDriver])

  override def transformationName = "spark"

  override def run(t: SparkTransformation): DriverRunHandle[SparkTransformation] = {
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

        if (master.startsWith("local"))
          l.setLocalTestMode()

    }

    new DriverRunHandle[SparkTransformation](this, new LocalDateTime(), t, l.startApplication())
  }


  override def getDriverRunState(run: DriverRunHandle[SparkTransformation]): DriverRunState[SparkTransformation] = {

    val appHandle = run.stateHandle.asInstanceOf[ExitCodeAwareChildProcAppHandle]
    val appInfo = run.transformation match {
      case SparkTransformation(
      applicationName, mainJarOrPy, mainClass, applicationArgs,
      _, _, _, _, _, _) =>
        s"$applicationName :: $mainJarOrPy ${if (mainClass != null) s" :: $mainClass" else ""} :: $applicationArgs"
    }

    appHandle.getState match {
      case FINISHED =>
        if (appHandle.getExitCode.isDefined && appHandle.getExitCode.head == 0)
          DriverRunSucceeded[SparkTransformation](this, s"Spark driver run succeeded for $appInfo")
        else if (appHandle.getExitCode.isEmpty)
          DriverRunFailed[SparkTransformation](this, s"Spark driver run failed for $appInfo", new RetryableDriverException(s"Exit code of Spark submit process is not available tho it is finished"))
        else
          DriverRunFailed[SparkTransformation](this, s"Spark driver run failed for $appInfo", new RetryableDriverException(s"Exit code of Spark submit process is 1"))
      case CONNECTED | SUBMITTED | RUNNING => DriverRunOngoing[SparkTransformation](this, run)

      case UNKNOWN => if (appHandle.childProc.isDefined)
        DriverRunOngoing[SparkTransformation](this, run)
      else
        DriverRunFailed[SparkTransformation](this, s"Spark driver run failed for $appInfo", new RetryableDriverException(s"Spark driver run was killed for $appInfo"))

      case _ => DriverRunFailed[SparkTransformation](this, s"Spark driver run failed for $appInfo", new RetryableDriverException(s"Spark driver run failed for $appInfo"))
    }
  }

  override def killRun(run: DriverRunHandle[SparkTransformation]) {
    val appHandle = run.stateHandle.asInstanceOf[SparkAppHandle]
    try {
      appHandle.kill()
    } catch {
      case t: Throwable => log.error("Spark driver failed to kill driver run", t)
    }
  }

  /**
    * Rig Spark transformation for test by setting master to local and deploy mode to client.
    */
  override def rigTransformationForTest(t: SparkTransformation, testResources: TestResources) = {
    val riggedT = t match {

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
      )
    }
    riggedT
  }
}

object SparkDriver extends DriverCompanionObject[SparkTransformation] {

  def apply(ds: DriverSettings) = {
    val conf = new HiveConf()

    conf.set("hive.metastore.local", "false")
    conf.setBoolVar(HiveConf.ConfVars.HIVESESSIONSILENT, true)
    conf.setVar(HiveConf.ConfVars.METASTOREURIS, Schedoscope.settings.metastoreUri.trim())

    if (Schedoscope.settings.kerberosPrincipal.trim() != "") {
      conf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL,
        true)
      conf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL,
        Schedoscope.settings.kerberosPrincipal)
    }

    new SparkDriver(ds.driverRunCompletionHandlers, conf)
  }

  def apply(driverSettings: DriverSettings, testResources: TestResources): Driver[SparkTransformation] =
    new SparkDriver(List("org.schedoscope.test.resources.TestDriverRunCompletionHandler"), testResources.hiveConf)
}