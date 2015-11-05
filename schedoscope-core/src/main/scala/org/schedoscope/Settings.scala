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
package org.schedoscope

import java.net.URLClassLoader
import java.nio.file.Paths
import java.util.Calendar
import java.util.concurrent.TimeUnit
import scala.Array.canBuildFrom
import scala.collection.mutable.HashMap
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.Duration
import scala.collection.JavaConversions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.schedoscope.scheduler.driver.FileSystemDriver.fileSystem
import org.schedoscope.dsl.Parameter.p
import org.schedoscope.dsl.transformations.Transformation
import org.schedoscope.dsl.views.DateParameterizationUtils
import com.typesafe.config.Config
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import org.schedoscope.scheduler.driver.Driver
import org.schedoscope.dsl.views.ViewUrlParser.ParsedViewAugmentor

/**
 * The Settings class keeps all settings for Schedoscope. It is a singelton accessible through
 * the companion object's apply() method.
 *
 * Configuration is based on TypeSafe config. For an explanation of the different
 * configuration settings, please refer to src/main/resources/reference.conf
 *
 */
class SchedoscopeSettings(val config: Config) extends Extension {

  private val driverSettings: HashMap[String, DriverSettings] = HashMap[String, DriverSettings]()

  /**
   * The configured Schedoscope environment
   */
  lazy val env = config.getString("schedoscope.app.environment")

  /**
   * The configured earliest day for DateParamterization logic
   */
  lazy val earliestDay = {
    val conf = config.getString("schedoscope.scheduler.earliestDay")
    val Array(year, month, day) = conf.split("-")
    DateParameterizationUtils.parametersToDay(p(year), p(month), p(day))
  }

  /**
   * The configured latest, i.e., current day for DateParamterization logic
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
   * The configured timeout for Schedoscope web service calls.
   */
  lazy val webserviceTimeout =
    Duration.create(config.getDuration("schedoscope.webservice.timeout", TimeUnit.MILLISECONDS),
      TimeUnit.MILLISECONDS)

  /**
   * The configured host of the Schedoscope web service (for calling via the SchedoscopeRestClient).
   */
  lazy val host = config.getString("schedoscope.webservice.host")

  /**
   * The configured post of the Schedoscope web service (for both web service and client)
   */
  lazy val port = config.getInt("schedoscope.webservice.port")

  /**
   * Configured directory for storing static web service resource data for Schedoscope web service
   */
  lazy val webResourcesDirectory = config.getString("schedoscope.webservice.resourceDirectory")

  /**
   * Configured actors for Schedoscope web service.
   */
  lazy val restApiConcurrency = config.getInt("schedoscope.webservice.concurrency")

  /**
   * Configured JDBC URL to the Hive server
   */
  lazy val jdbcUrl = config.getString("schedoscope.metastore.jdbcUrl")

  /**
   * Configured kerberos principal.
   */
  lazy val kerberosPrincipal = config.getString("schedoscope.kerberos.principal")

  /**
   * Configured Thrift metastore URI.
   */
  lazy val metastoreUri = config.getString("schedoscope.metastore.metastoreUri")

  /**
   * Configured view augmentor class for postprocessing of views after URL parsing
   */
  lazy val parsedViewAugmentorClass = config.getString("schedoscope.app.parsedViewAugmentorClass")

  /**
   * An instance of the view augmentor class
   */
  lazy val viewAugmentor = Class.forName(parsedViewAugmentorClass).newInstance().asInstanceOf[ParsedViewAugmentor]

  /**
   * The subconfigurations of the configured transformation types.
   */
  lazy val availableTransformations = config.getObject("schedoscope.transformations")

  /**
   * A suitable hadoop config to use
   */
  lazy val hadoopConf = new Configuration(true)

  /**
   * Configuration trigger whether versioning transformation is enabled.
   */
  lazy val transformationVersioning = config.getBoolean("schedoscope.versioning.transformations")

  /**
   * Return the resource manager or job tracker equivalent
   */
  lazy val jobTrackerOrResourceManager = {
    val yarnConf = new YarnConfiguration(hadoopConf)
    if (yarnConf.get("yarn.resourcemanager.address") == null)
      config.getString("schedoscope.hadoop.resourceManager")
    else
      yarnConf.get("yarn.resourcemanager.address")
  }

  /**
   * The configured HDFS namenode. The Hadoop default takes precendence.
   */
  lazy val nameNode = if (hadoopConf.get("fs.defaultFS") == null)
    config.getString("schedoscope.hadoop.nameNode")
  else
    hadoopConf.get("fs.defaultFS")

  /**
   * The configured timeout for filesystem operations.
   */
  lazy val filesystemTimeout = getDriverSettings("filesystem").timeout

  /**
   * The configured timeout for schema / metastore operations.
   */
  lazy val schemaTimeout = Duration.create(config.getDuration("schedoscope.scheduler.timeouts.schema", TimeUnit.SECONDS), TimeUnit.SECONDS)

  /**
   * The configured timeout for collecting status lists (view / transformation)
   */
  lazy val statusListAggregationTimeout = Duration.create(config.getDuration("schedoscope.scheduler.timeouts.statusListAggregation", TimeUnit.SECONDS), TimeUnit.SECONDS)

  /**
   * The configured timout for responses of the view manager.
   */
  lazy val viewManagerResponseTimeout = Duration.create(config.getDuration("schedoscope.scheduler.timeouts.viewManagerResponse", TimeUnit.SECONDS), TimeUnit.SECONDS)

  /**
   * Timeout for completion of materialization commands
   */
  lazy val completitionTimeout = Duration.create(config.getDuration("schedoscope.scheduler.timeouts.completion", TimeUnit.SECONDS), TimeUnit.SECONDS)

  /**
   * The configured number of retries before a view enters failed state.
   */
  lazy val retries = config.getInt("schedoscope.action.retry")

  /**
   * Number of parallel threads to access the metastore
   */
  lazy val metastoreConcurrency = config.getInt("schedoscope.metastore.concurrency")

  /**
   * Number of partitions to write in batch to the metatatore
   */
  lazy val metastoreWriteBatchSize = config.getInt("schedoscope.metastore.writeBatchSize")

  /**
   * Number of partitions to read in batch to the metatatore
   */
  lazy val metastoreReadBatchSize = config.getInt("schedoscope.metastore.readBatchSize")

  /**
   * A user group information object ready to use for kerberized interactions.
   */
  lazy val userGroupInformation = {
    UserGroupInformation.setConfiguration(hadoopConf)
    val ugi = UserGroupInformation.getCurrentUser()
    ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS)
    ugi.reloginFromKeytab();
    ugi
  }

  /**
   * Returns driver-specific settings from the configuration
   *
   * @param d  driver to retrieve the settings for
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
   * @param nt the name of the transformation type (e.g. mapreduce)
   * @return the driver settings
   */
  def getDriverSettings(nt: String): DriverSettings = {
    if (!driverSettings.contains(nt)) {
      val confName = "schedoscope.transformations." + nt
      driverSettings.put(nt, new DriverSettings(config.getConfig(confName), nt))
    }

    driverSettings(nt)
  }

  /**
   * Retrieve a setting  for a transformation type
   *
   * @param nt	the name of the transformation type (e.g. mapreduce)
   * @param n	the name of the setting for nt
   * @return the setting's value as a string
   */
  def getTransformationSetting(bt: String, n: String) = {
    val confName = s"schedoscope.transformations.${bt}.transformation.${n}"
    config.getString(confName)
  }

}

/**
 * Companion object for settings
 */
object Settings extends ExtensionId[SchedoscopeSettings] with ExtensionIdProvider {
  override def lookup = Settings

  override def createExtension(system: ExtendedActorSystem) =
    new SchedoscopeSettings(system.settings.config)

  override def get(system: ActorSystem): SchedoscopeSettings = super.get(system)

  def apply() = {
    super.apply(Schedoscope.actorSystem)
  }
}

/**
 * Accessor for transformation driver-related settings.
 */
class DriverSettings(val config: Config, val name: String) {
  /**
   * Location where to put transformation-driver related resources into HDFS upon Schedoscope start.
   */
  lazy val location = Settings().nameNode + config.getString("location") + Settings().env + "/"

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
    val fromLibDir = libDirectory
      .split(",")
      .toList
      .filter(!_.trim.equals(""))
      .map(p => { if (!p.endsWith("/")) s"file://${p.trim}/*" else s"file://${p.trim}*" })
      .flatMap(dir => {
        fileSystem(dir, Settings().hadoopConf).globStatus(new Path(dir))
          .map(stat => stat.getPath.toString)
      })

    val fromClasspath = this.getClass.getClassLoader
      .asInstanceOf[URLClassLoader]
      .getURLs
      .map(el => el.toString)
      .distinct
      .filter(_.endsWith(s"-${name}.jar"))
      .toList

    (fromLibDir ++ fromClasspath).toList
  }

  /**
   * Paths of jars in HDFS.
   */
  lazy val libJarsHdfs = {
    if (unpack)
      List[String]()
    else {
      libJars.map(lj => location + "/" + Paths.get(lj).getFileName.toString)
    }
  }

  /**
   * Configured driver run completion handlers.
   */
  lazy val driverRunCompletionHandlers = config.getStringList("driverRunCompletionHandlers").toList
}
