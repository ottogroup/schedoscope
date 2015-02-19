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

class SettingsImpl(val config: Config) extends Extension with defaults {

  println(config)
    
  val system = Settings.actorSystem
  
  val sodaJar = this.getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath
  
  val env: String = get(config, "soda.app.environment", "dev")

  val webserviceTimeOut: Duration =
  Duration(config.getDuration("soda.webservice.timeout", TimeUnit.MILLISECONDS),
      TimeUnit.MILLISECONDS)

  val port: Int = get(config, "soda.webservice.port", 20698)

  val packageName: String = get(config, "soda.app.package", "app.eci")

  val jdbcUrl: String = get(config, "soda.metastore.jdbcUrl", "")

  val kerberosPrincipal = get(config, "soda.kerberos.principal", "")

  val metastoreUri = get(config, "soda.metastore.metastoreUri", "")

  val parsedViewAugmentorClass = get(config, "soda.app.parsedViewAugmentorClass", "")
  
  val libDirectory = get(config, "soda.app.libDirectory", sodaJar.replaceAll("/[^/]+$", "/"))
    
  val availableTransformations = config.getObject("soda.transformations")
    
  val hadoopConf = {  
    val hc = new Configuration(true)
    hc.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"))
    hc.addResource(new Path("/etc/hadoop/conf/core-site.xml"))
    hc
  }
     
  val userGroupInformation = {
      UserGroupInformation.setConfiguration(hadoopConf)
      val ugi = UserGroupInformation.getCurrentUser()
      ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS)
      ugi.reloginFromKeytab();
      ugi
      
  }
  
  private val driverSettings : HashMap[String,DriverSettings] = HashMap[String,DriverSettings]()
  
  def getSettingsForDriver(d: Any with Driver) : DriverSettings = {
    if (!driverSettings.contains(d.name)) {     
      val confName = "soda.transformations." + d.name
      driverSettings.put(d.name, new DriverSettings(get(config, confName, ConfigFactory.empty()), d.name))
    }
    driverSettings(d.name)
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

class DriverSettings(val config: Config, val name: String) extends defaults {
  val location = get(config, "location", "/tmp/soda/"+name)
  val libDirectory = get(config, "libDirectory", "")
  val concurrency = get(config, "concurrency", 1)
  val unpack = get(config, "unpack", false)
  var libJars = List[String]()
}


trait defaults {
  def get[T](conf: Config, p : String, d : T) : T  = {
    if (conf.hasPath(p)) {
      d match {
        case v : String => conf.getString(p).asInstanceOf[T]
        case v : Int => conf.getInt(p).asInstanceOf[T]
        case v : Config => conf.getObject(p).toConfig.asInstanceOf[T]
        case v : Boolean => conf.getBoolean(p).asInstanceOf[T]
        case _ => conf.getObject(p).asInstanceOf[T]
      }
    }
    else {
      d
    }
  }

  
  
}
