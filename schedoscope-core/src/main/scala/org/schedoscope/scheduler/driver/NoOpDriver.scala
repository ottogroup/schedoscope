package org.schedoscope.scheduler.driver

import org.joda.time.LocalDateTime
import org.schedoscope.conf.DriverSettings
import org.schedoscope.dsl.transformations.NoOp
import org.schedoscope.test.resources.TestResources

class NoOpDriver(val driverRunCompletionHandlerClassNames: List[String]) extends DriverOnBlockingApi[NoOp] {
  /**
    * The name of the transformations executed by this driver.
    */
  override def transformationName: String = "noop"

  /**
    * Create a driver run, i.e., start the execution of the transformation asychronously.
    */
  override def run(t: NoOp): DriverRunHandle[NoOp] = {
    new DriverRunHandle[NoOp](this, new LocalDateTime(), t, DriverRunSucceeded(this, "what did you expect?"))
  }
}

object NoOpDriver extends DriverCompanionObject[NoOp] {
  /**
    * Construct the driver from its settings. The settings are picked up via the name of the driver
    * from the configurations
    *
    * @param driverSettings the driver settings
    * @return the instantiated driver
    */
  override def apply(driverSettings: DriverSettings): Driver[NoOp] = {
    new NoOpDriver(driverSettings.driverRunCompletionHandlers)
  }

  /**
    * Construct the driver from its settings in the context of the Schedoscope test framework.
    *
    * @param driverSettings the driver settings
    * @param testResources  the resources within the test environment
    * @return the instantiated test driver
    */
  override def apply(driverSettings: DriverSettings, testResources: TestResources): Driver[NoOp] = {
    new NoOpDriver(List("org.schedoscope.test.resources.TestDriverRunCompletionHandler"))
  }
}
