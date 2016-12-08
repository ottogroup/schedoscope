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
package org.schedoscope.conf

import java.net.URLClassLoader
import java.nio.file.Paths
import java.util.Calendar
import java.util.concurrent.TimeUnit

import akka.actor.Extension
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.schedoscope.Schedoscope
import org.schedoscope.dsl.Parameter.p
import org.schedoscope.dsl.transformations.Transformation
import org.schedoscope.dsl.views.DateParameterizationUtils
import org.schedoscope.dsl.views.ViewUrlParser.ParsedViewAugmentor
import org.schedoscope.scheduler.driver.Driver
import org.schedoscope.scheduler.driver.FilesystemDriver.fileSystem

import scala.Array.canBuildFrom
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import scala.concurrent.duration.Duration

/**
  *
  */
class SchedoscopeSettings(config: Config) extends BaseSettings(config) with Extension {

  private val driverSettings: HashMap[String, DriverSettings] = HashMap[String, DriverSettings]()

  /**
    * The configured earliest day for DateParamterization logic
    */
  lazy val earliestDay = {
    val conf = config.getString("schedoscope.scheduler.earliestDay")
    val Array(year, month, day) = conf.split("-")
    DateParameterizationUtils.parametersToDay(p(year), p(month), p(day))
  }

  /**
    * The configured latest, i.e., current day for DateParameterization logic
    */
  def latestDay = {
    val conf = config.getString("schedoscope.scheduler.latestDay")
    if (conf == "now") {
      val now = Calendar.getInstance()
      now.set(Calendar.HOUR_OF_DAY, 0)
      now.set(Calendar.MINUTE, 0)
      now.set(Calendar.SECOND, 0)
      now.set(Calendar.MILLISECOND, 0)
      now
    } else {
      val Array(year, month, day) = conf.split("-")
      DateParameterizationUtils.parametersToDay(p(year), p(month), p(day))
    }
  }

  /**
    * Configured view action scheduling listener handlers.
    */
  lazy val viewSchedulingRunCompletionHandlers = {
    try
      config.getStringList("schedoscope.scheduler.listeners.viewSchedulingRunCompletionHandlers").toList
    catch {
      case _: Throwable => List()
    }
  }

  lazy val viewScheduleListenerActorsMaxRetries = {
    try
      config.getInt("akka.actor.view-scheduling-listener-actor.maxRetries")
    catch {
      case _: Throwable => -1
    }
  }

    /**
      * An instance of the view augmentor class
      */
    lazy val viewAugmentor = Class.forName(parsedViewAugmentorClass).newInstance().asInstanceOf[ParsedViewAugmentor]

    /**
      * A suitable hadoop config to use
      */
    lazy val hadoopConf = {
      val conf = new Configuration(true)
      if (conf.get("fs.defaultFS") == null)
        conf.set("fs.defaultFS", config.getString("schedoscope.hadoop.nameNode"))
      conf
    }

    /**
      * The configured HDFS namenode. The Hadoop default takes precendence.
      */
    lazy val nameNode = hadoopConf.get("fs.defaultFS")

    /**
      * Return the resource manager or job tracker equivalent. The Hadoop configuration takes precendence.
      */
    lazy val jobTrackerOrResourceManager = {
      val yarnConf = new YarnConfiguration(hadoopConf)
      if (yarnConf.get("yarn.resourcemanager.address") == null)
        config.getString("schedoscope.hadoop.resourceManager")
      else
        yarnConf.get("yarn.resourcemanager.address")
    }

    /**
      * The configured timeout for filesystem operations.
      */
    lazy val filesystemTimeout = getDriverSettings("filesystem").timeout

    /**
      * A user group information object ready to use for kerberized interactions.
      */
    def userGroupInformation = {
      UserGroupInformation.setConfiguration(hadoopConf)
      val ugi = UserGroupInformation.getCurrentUser()
      ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS)
      ugi.reloginFromKeytab()
      ugi
    }

    /**
      * Returns driver-specific settings from the configuration
      *
      * @param d driver to retrieve the settings for
      * @return the driver settings
      */
    def getDriverSettings(d: Any with Driver[_]): DriverSettings = {
      getDriverSettings(d.transformationName)
    }

    /**
      * Returns driver-specific settings from the configuration by transformation type
      *
      * @param t transformation type whose settings are of interest
      * @return the driver settings
      */
    def getDriverSettings[T <: Transformation](t: T): DriverSettings = {
      val name = t.getClass.getSimpleName.toLowerCase.replaceAll("transformation", "").replaceAll("\\$", "")
      getDriverSettings(name)
    }

    /**
      * Returns driver-specific settings by transformation type name
      *
      * @param transformationName the name of the transformation type (e.g. mapreduce)
      * @return the driver settings
      */
    def getDriverSettings(transformationName: String): DriverSettings = {
      if (!driverSettings.contains(transformationName)) {
        val confName = "schedoscope.transformations." + transformationName
        driverSettings.put(transformationName, new DriverSettings(config.getConfig(confName), transformationName))
      }

      driverSettings(transformationName)
    }

    /**
      * Retrieve a setting  for a transformation type
      *
      * @param transformationName the name of the transformation type (e.g. mapreduce)
      * @param n                  the name of the setting for transformationName
      * @return the setting's value as a string
      */
    def getTransformationSetting(transformationName: String, n: String) = {
      val confName = s"schedoscope.transformations.${transformationName}.transformation.${n}"
      config.getString(confName)
    }

}

/**
  * Accessor for transformation driver-related settings.
  */
class DriverSettings(val config: Config, val name: String) {

  /**
    * Name of the class implementing the driver
    */
  lazy val driverClassName = config.getString("driverClassName")

  /**
    * Location where to put transformation-driver related resources into HDFS upon Schedoscope start.
    */
  lazy val location = Schedoscope.settings.nameNode + config.getString("location") + Schedoscope.settings.env + "/"

  /**
    * Comma-separated list of additional jars to put in HDFS location upon Schedoscope start.
    */
  lazy val libDirectory = config.getString("libDirectory")

  /**
    * Number of parallel drivers.
    */
  lazy val concurrency = config.getInt("concurrency")

  /**
    * Do the driver respective resource need unpacking
    */
  lazy val unpack = config.getBoolean("unpack")

  /**
    * URL for the driver to access the service ultimately executing the transformation, e.g., the hive server.
    */
  lazy val url = config.getString("url")

  /**
    * Timeout for transformation of this type.
    */
  lazy val timeout = Duration.create(config.getDuration("timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  /**
    * List of jars to upload to HDFS.
    */
  lazy val libJars = {
    val fromLibDir = try {
      libDirectory
        .split(",")
        .toList
        .filter(!_.trim.equals(""))
        .map(p => {
          if (!p.endsWith("/")) s"file://${p.trim}/*" else s"file://${p.trim}*"
        })
        .flatMap(dir => {
          fileSystem(dir, Schedoscope.settings.hadoopConf).globStatus(new Path(dir))
            .map(stat => stat.getPath.toString)
        })
    } catch {
      case _: Throwable => List()
    }

    val fromClasspath = this.getClass.getClassLoader
      .asInstanceOf[URLClassLoader]
      .getURLs
      .map(el => el.toString)
      .distinct
      .filter(_.endsWith(s"-$name.jar"))
      .toList

    fromLibDir ++ fromClasspath
  }

  /**
    * Paths of jars in HDFS.
    */
  lazy val libJarsHdfs =
    if (unpack)
      List[String]()
    else
      libJars.map(lj => location + "/" + Paths.get(lj).getFileName.toString)

  /**
    * Configured driver run completion handlers.
    */
  lazy val driverRunCompletionHandlers = config.getStringList("driverRunCompletionHandlers").toList

}
