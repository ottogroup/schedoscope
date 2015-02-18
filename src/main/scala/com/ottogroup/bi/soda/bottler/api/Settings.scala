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
import com.ottogroup.bi.soda.bottler.driver.FileSystemDriver

class SettingsImpl(config: Config) extends Extension with defaults {

  conf = config
  println(config)
  def getConfig=config
  
  val sodaJar = this.getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath
  
  val env: String = get("soda.app.environment", "dev")

  val webserviceTimeOut: Duration =
  Duration(config.getDuration("soda.webservice.timeout", TimeUnit.MILLISECONDS),
      TimeUnit.MILLISECONDS)

  val port: Int = get("soda.webservice.port", 20698)

  val packageName: String = get("soda.app.package", "app.eci")

  val jdbcUrl: String = get("soda.hive.jdbcUrl", "")

  val kerberosPrincipal = get("soda.kerberos.principal", "")

  val metastoreUri = get("soda.hive.metastoreUri", "")

  //val oozieUri = config.getString("soda.oozie.url")

  val parsedViewAugmentorClass = get("soda.app.parsedViewAugmentorClass", "")
  
  val libDirectory = get("soda.app.libDirectory", sodaJar.replaceAll("/[^/]+$", "/"))
    
  val availableTransformations = get("soda.transformations", ConfigFactory.empty())
    
  val hadoopConf = {  
    val hc = new Configuration(true)
    hc.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"))
    hc.addResource(new Path("/etc/hadoop/conf/core-site.xml"))
    hc
  }
  
  val system = Settings.actorSystem
  
  val userGroupInformation = {
      UserGroupInformation.setConfiguration(hadoopConf)
      val ugi = UserGroupInformation.getCurrentUser()
      ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS)
      ugi.reloginFromKeytab();
      ugi
      
  } 
  
  def getSettingsForDriver(d: Any) : DriverSettings = {
    val confName = "soda.transformations." + d.getClass.getSimpleName.toLowerCase.replaceAll("driver", "")
    new DriverSettings(get(confName, ConfigFactory.empty()))
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

class DriverSettings(config: Config) extends defaults {
  conf = config
  val location = get("location", "/tmp/soda")
  val libDirectory = get("libDirectory", "")
  val concurrency = get("concurrency", 1)
  val unpack = get("unpack", false)
}


trait defaults {
  var conf : Config = ConfigFactory.empty()
  def get[T](p : String, d : T)  = {
    if (conf.hasPath(p)) {
      d match {
        case v : String => conf.getString(p)
        case v : Int => conf.getInt(p)
        case v : Config => conf.getObject(p).toConfig
        case _ => conf.getObject(p)
      }
    }
    d
  }

  
  
}
