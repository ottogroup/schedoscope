package org.schedoscope

import com.typesafe.config.ConfigFactory
import org.schedoscope.conf.SchedoscopeSettings

object TestUtils {

  def createSettings(customSettings: String*): SchedoscopeSettings = {
    val myConfig =
      ConfigFactory.parseString(customSettings.mkString("\n"))
    // load the normal config stack (system props,
    // then application.conf, then reference.conf)
    val regularConfig =
    ConfigFactory.load()
    // override regular stack with myConfig
    val combined =
      myConfig.withFallback(regularConfig)
    // put the result in between the overrides
    // (system props) and defaults again
    val complete = ConfigFactory.load(combined)

    Settings(complete)
  }


}
