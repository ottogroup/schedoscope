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

class SettingsImpl(config: Config) extends Extension {
  
  
  println(config)
  def getConfig=config
  val env: String = config.getString("soda.app.environment")

  val webserviceTimeOut: Duration =
    Duration(config.getMilliseconds("soda.webservice.timeout"),
      TimeUnit.MILLISECONDS)

  val port: Int = config.getInt("soda.webservice.port")

  val packageName: String = config.getString("soda.app.package")

  val jdbcUrl: String = config.getString("soda.metastore.jdbcUrl")

  val kerberosPrincipal = config.getString("soda.kerberos.principal")

  val metastoreUri = config.getString("soda.metastore.metastoreUri")

  //val oozieUri = config.getString("soda.oozie.url")

  val parsedViewAugmentorClass = config.getString("soda.app.parsedViewAugmentorClass")
  
  val libDirectory = config.getString("soda.app.libDirectory")
  
  val udfJar = config.getString("soda.transformations.hive.udfJar")
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
}

object Settings extends ExtensionId[SettingsImpl] with ExtensionIdProvider {
  
   

  override def lookup = Settings

  override def createExtension(system: ExtendedActorSystem) =
    new SettingsImpl(system.settings.config)

  override def get(system: ActorSystem): SettingsImpl = super.get(system)
}