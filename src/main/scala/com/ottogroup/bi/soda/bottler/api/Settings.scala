package com.ottogroup.bi.soda.bottler.api

import akka.actor.ActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.actor.ExtendedActorSystem
import scala.concurrent.duration.Duration
import com.typesafe.config.Config
import java.util.concurrent.TimeUnit

class SettingsImpl(config: Config) extends Extension {

  println(config)

  val env: String = config.getString("soda.app.environment")

  val webserviceTimeOut: Duration =
    Duration(config.getMilliseconds("soda.webservice.timeout"),
      TimeUnit.MILLISECONDS)

  val port: Int = config.getInt("soda.webservice.port")

  val packageName: String = config.getString("soda.app.package")

  val jdbcUrl: String = config.getString("soda.hive.jdbcUrl")

  val kerberosPrincipal = config.getString("soda.kerberos.principal")

  val metastoreUri = config.getString("soda.hive.metastoreUri")

  val oozieUri = config.getString("soda.oozie.url")

  val parsedViewAugmentorClass = config.getString("soda.app.parsedViewAugmentorClass")
}

object Settings extends ExtensionId[SettingsImpl] with ExtensionIdProvider {

  override def lookup = Settings

  override def createExtension(system: ExtendedActorSystem) =
    new SettingsImpl(system.settings.config)

  override def get(system: ActorSystem): SettingsImpl = super.get(system)
}