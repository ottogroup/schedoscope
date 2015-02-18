package com.ottogroup.bi.soda.bottler.driver

import com.ottogroup.bi.soda.dsl.Transformation
import com.typesafe.config.Config
import com.ottogroup.bi.soda.bottler.api.Settings
import com.ottogroup.bi.soda.bottler.api.DriverSettings

trait Driver {
  var driverSettings = Settings().getSettingsForDriver(this)
  // non-blocking
  def run(t: Transformation): String
  // blocking
  def runAndWait(t: Transformation): Boolean
  // deploy resources for a single transformation FIXME: this is the next step
  // def deploy(t: Transformation, f: FileSystemDriver, c: Config) : Boolean
  // deploy resources for all transformations run by this driver
  def deployAll() : Boolean = {
    driverSettings.
    true
  }  
}