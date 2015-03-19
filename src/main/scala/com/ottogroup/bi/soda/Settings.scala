package com.ottogroup.bi.soda

import java.net.URLClassLoader
import java.nio.file.Paths
import java.util.Calendar
import java.util.concurrent.TimeUnit
import scala.Array.canBuildFrom
import scala.collection.mutable.HashMap
import scala.concurrent.duration.Duration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.conf.YarnConfiguration
import com.ottogroup.bi.soda.bottler.driver.FileSystemDriver.fileSystem
import com.ottogroup.bi.soda.dsl.Parameter.p
import com.typesafe.config.Config
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import com.ottogroup.bi.soda.dsl.views.DateParameterizationUtils
import com.ottogroup.bi.soda.bottler.driver.Driver
import com.ottogroup.bi.soda.dsl.Transformation

class SettingsImpl(val config: Config) extends Extension {
  val system = Settings.actorSystem

  private val driverSettings: HashMap[String, DriverSettings] = HashMap[String, DriverSettings]()
  
  val env = config.getString("soda.app.environment")

  val earliestDay = {
    val conf = config.getString("soda.scheduler.earliestDay")
    val Array(year, month, day) = conf.split("-")
    DateParameterizationUtils.parametersToDay(p(year), p(month), p(day))
  }

  val latestDay = {
    val conf = config.getString("soda.scheduler.latestDay")
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

  val webserviceTimeOut: Duration =
  Duration(config.getDuration("soda.webservice.timeout", TimeUnit.MILLISECONDS),
      TimeUnit.MILLISECONDS)

  val port = config.getInt("soda.webservice.port")

  val jdbcUrl = config.getString("soda.metastore.jdbcUrl")

  val kerberosPrincipal = config.getString("soda.kerberos.principal")

  val metastoreUri = config.getString("soda.metastore.metastoreUri")

  val parsedViewAugmentorClass = config.getString("soda.app.parsedViewAugmentorClass")

  val availableTransformations = config.getObject("soda.transformations")

  val hadoopConf = new Configuration(true)

  val transformationVersioning = config.getBoolean("soda.versioning.transformations")

  val jobTrackerOrResourceManager = {
    val yarnConf = new YarnConfiguration(hadoopConf)
    if (yarnConf.get("yarn.resourcemanager.address") == null)
      config.getString("soda.hadoop.resourceManager")
    else
      yarnConf.get("yarn.resourcemanager.address")
  }

  val nameNode = if (hadoopConf.get("fs.defaultFS") == null)
    config.getString("soda.hadoop.nameNode")
  else
    hadoopConf.get("fs.defaultFS")

  def filesystemTimeout = getDriverSettings("filesystem").timeout
  val schemaTimeout = Duration.create(config.getDuration("soda.scheduler.timeouts.schema", TimeUnit.SECONDS), TimeUnit.SECONDS)
  val statusListAggregationTimeout = Duration.create(config.getDuration("soda.scheduler.timeouts.statusListAggregation", TimeUnit.SECONDS), TimeUnit.SECONDS)
  val viewManagerResponseTimeout = Duration.create(config.getDuration("soda.scheduler.timeouts.viewManagerResponse", TimeUnit.SECONDS), TimeUnit.SECONDS)
  val completitionTimeout = Duration.create(config.getDuration("soda.scheduler.timeouts.completion", TimeUnit.SECONDS), TimeUnit.SECONDS)
  
  val retries = config.getInt("soda.action.retry")

  val userGroupInformation = {
    UserGroupInformation.setConfiguration(hadoopConf)
    val ugi = UserGroupInformation.getCurrentUser()
    ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS)
    ugi.reloginFromKeytab();
    ugi
  }
  

  def getDriverSettings(d: Any with Driver[_]): DriverSettings = {
    getDriverSettings(d.transformationName)
  }

  def getDriverSettings[T <: Transformation](t: T): DriverSettings = {
    val name = t.getClass.getSimpleName.toLowerCase.replaceAll("transformation", "").replaceAll("\\$", "")
    getDriverSettings(name)
  }
  
  def getDriverSettings(n : String) : DriverSettings = {
    if (!driverSettings.contains(n)) {     
      val confName = "soda.transformations." + n
      driverSettings.put(n, new DriverSettings(config.getConfig(confName), n))
    }

    driverSettings(n)
  }
  
  def getTransformationSetting(typ: String, setting: String) = {
    val confName = s"soda.transformations.${typ}.transformation.${setting}"
    config.getString(confName)
  }
  
}

object Settings extends ExtensionId[SettingsImpl] with ExtensionIdProvider {
  val actorSystem = ActorSystem("soda")

  override def lookup = Settings

  override def createExtension(system: ExtendedActorSystem) =
    new SettingsImpl(system.settings.config)

  override def get(system: ActorSystem): SettingsImpl = super.get(system)

  def apply() = {
    super.apply(actorSystem)
  }
}

class DriverSettings(val config: Config, val name: String) {
  val location = Settings().nameNode + config.getString("location")
  val libDirectory = config.getString("libDirectory")
  val concurrency = config.getInt("concurrency")
  val unpack = config.getBoolean("unpack")
  val url = config.getString("url")
  val timeout = Duration.create(config.getDuration("timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)
  
  val libJars = {
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

  val libJarsHdfs = {
    if (unpack)
      List[String]()
    else {
      libJars.map(lj => location + "/" + Paths.get(lj).getFileName.toString)
    }
  }
}

