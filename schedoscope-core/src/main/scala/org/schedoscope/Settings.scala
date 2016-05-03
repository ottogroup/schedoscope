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
import akka.actor.{ ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider }
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.schedoscope.dsl.Parameter.p
import org.schedoscope.dsl.transformations.Transformation
import org.schedoscope.dsl.views.DateParameterizationUtils
import org.schedoscope.dsl.views.ViewUrlParser.ParsedViewAugmentor
import org.schedoscope.scheduler.driver.Driver
import org.schedoscope.scheduler.driver.FileSystemDriver.fileSystem
import scala.Array.canBuildFrom
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import scala.concurrent.duration.Duration
import org.schedoscope.conf.SchedoscopeSettings
 
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
