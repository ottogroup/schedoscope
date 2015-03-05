package com.ottogroup.bi.soda.bottler.api

import akka.actor.ActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.actor.ExtendedActorSystem
import scala.concurrent.duration.Duration
import com.typesafe.config.Config
import java.util.concurrent.TimeUnit
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.security.UserGroupInformation
import com.ottogroup.bi.soda.bottler.driver.HiveDriver
import com.ottogroup.bi.soda.bottler.driver.OozieDriver
import com.ottogroup.bi.soda.bottler.driver.Driver
import com.ottogroup.bi.soda.bottler.driver.FileSystemDriver
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import java.util.Properties
import java.io.FileReader
import java.io.IOException
import java.nio.file.Paths
import com.ottogroup.bi.soda.dsl.Transformation
import java.net.URLClassLoader
import FileSystemDriver._
import org.apache.hadoop.yarn.conf.YarnConfiguration

class SettingsImpl(val config: Config) extends Extension {

  println(config)
    
  val system = Settings.actorSystem

  val env = config.getString("soda.app.environment")

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
    
  val hiveActionTimeout = config.getDuration("soda.timeouts.hive", TimeUnit.SECONDS)
  val oozieActionTimeout = config.getDuration("soda.timeouts.oozie", TimeUnit.SECONDS)
  val fileActionTimeout = config.getDuration("soda.timeouts.file", TimeUnit.SECONDS)
  val schemaActionTimeout = config.getDuration("soda.timeouts.schema", TimeUnit.SECONDS)
  val dependencyTimout = config.getDuration("soda.timeouts.dependency", TimeUnit.SECONDS)
  val materializeAllTimeout = config.getDuration("soda.timeouts.all", TimeUnit.SECONDS)

  val hiveActionTimeout = Duration.create(config.getDuration("soda.timeouts.hive",TimeUnit.SECONDS),TimeUnit.SECONDS)
  val oozieActionTimeout =  Duration.create(config.getDuration("soda.timeouts.oozie",TimeUnit.SECONDS),TimeUnit.SECONDS)
  val fileActionTimeout =  Duration.create(config.getDuration("soda.timeouts.file",TimeUnit.SECONDS),TimeUnit.SECONDS)
  val schemaActionTimeout =  Duration.create(config.getDuration("soda.timeouts.schema",TimeUnit.SECONDS),TimeUnit.SECONDS)
  val dependencyTimout =  Duration.create(config.getDuration("soda.timeouts.dependency",TimeUnit.SECONDS),TimeUnit.SECONDS)
  val materializeAllTimeout =  Duration.create(config.getDuration("soda.timeouts.all",TimeUnit.SECONDS),TimeUnit.SECONDS)
  val retries = config.getInt("soda.action.retry")
  val userGroupInformation = {
    UserGroupInformation.setConfiguration(hadoopConf)
    val ugi = UserGroupInformation.getCurrentUser()
    ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS)
    ugi.reloginFromKeytab();
    ugi
  }

  private val driverSettings: HashMap[String, DriverSettings] = HashMap[String, DriverSettings]()

  def getDriverSettings(d: Any with Driver): DriverSettings = {
    getDriverSettings(d.name)
  } 
      
  def getDriverSettings[T <: Transformation](t : T) : DriverSettings = {
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
}

object Settings extends ExtensionId[SettingsImpl] with ExtensionIdProvider {
  val actorSystem = ActorSystem("sodaSystem")
  
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
