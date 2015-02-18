package com.ottogroup.bi.soda.bottler.driver

import com.ottogroup.bi.soda.dsl.Transformation
import com.typesafe.config.Config
import com.ottogroup.bi.soda.bottler.api.Settings
import com.ottogroup.bi.soda.bottler.api.DriverSettings
import com.ottogroup.bi.soda.bottler.api.DriverSettings
import java.net.URLClassLoader

trait Driver {
  var driverSettings = Settings().getSettingsForDriver(this)
  
  // non-blocking
  def run(t: Transformation): String  
  // blocking
  
  def runAndWait(t: Transformation): Boolean
  // deploy resources for a single transformation FIXME: this is the next step
  // def deploy(t: Transformation, f: FileSystemDriver, c: Config) : Boolean
  // deploy resources for all transformations run by this driver
  
  def name = this.getClass.getSimpleName.toLowerCase.replaceAll("driver", "")
  
  def deployAll() : Boolean = {
    val fsd = new FileSystemDriver(Settings().userGroupInformation, Settings().hadoopConf)
    
    // clear destination
    fsd.delete(driverSettings.location, true)
    fsd.mkdirs(driverSettings.location)
    
    // upload jars from lib directory FIXME: unpack
    val succLibDir = driverSettings.libDirectory
      .split(",")
      .toList
      .map (dir => {
        // FIXME: wildcards
        println("Copying file://" + dir + "*" + " to " + driverSettings.location)
        fsd.copy("file://" + dir, driverSettings.location, true)
      })
      .reduceOption((a,b) => a && b)
      
    // upload jars matching name pattern from classpath (e.g. eci-views-hive.jar)
    val classPathMembers = this.getClass.getClassLoader.asInstanceOf[URLClassLoader].getURLs.map { _.toString() }.distinct
    val succClasspath = classPathMembers.filter { _.endsWith(s"-${name}.jar") }
                                        .toList
                                        .map( el => {
                                          println("Copying " + el + " to " + driverSettings.location)
                                          fsd.copy(el, driverSettings.location, false)
                                        })
                                        .reduceOption((a,b) => a && b)
                                        
    succLibDir.getOrElse(false) && succClasspath.getOrElse(false)
  }  
}