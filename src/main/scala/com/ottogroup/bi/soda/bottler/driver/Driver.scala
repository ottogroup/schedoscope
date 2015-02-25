package com.ottogroup.bi.soda.bottler.driver

import com.ottogroup.bi.soda.dsl.Transformation
import com.typesafe.config.Config
import com.ottogroup.bi.soda.bottler.api.Settings
import com.ottogroup.bi.soda.bottler.api.DriverSettings
import com.ottogroup.bi.soda.bottler.api.DriverSettings
import java.net.URLClassLoader
import scala.collection.mutable.ListBuffer
import java.nio.file.Files
import scala.util.Random
import net.lingala.zip4j.core.ZipFile

trait Driver {
  // non-blocking
  def run(t: Transformation): String

  // blocking
  def runAndWait(t: Transformation): Boolean
  // deploy resources for a single transformation FIXME: this is the next step
  // def deploy(t: Transformation, f: FileSystemDriver, c: Config) : Boolean
  // deploy resources for all transformations run by this driver

  def name = this.getClass.getSimpleName.toLowerCase.replaceAll("driver", "")

  def deployAll(driverSettings: DriverSettings): Boolean = {
    val fsd = new FileSystemDriver(Settings().userGroupInformation, Settings().hadoopConf)

    // clear destination
    fsd.delete(driverSettings.location, true)
    fsd.mkdirs(driverSettings.location)

    val succ = driverSettings.libJars
      .map(f => {
        if (driverSettings.unpack) {
          val tmpDir = Files.createTempDirectory("soda-" + Random.nextLong.abs.toString).toFile
          println(s"Unzipping ${name} resource ${f}")
          new ZipFile(f.replaceAll("file:", "")).extractAll(tmpDir.getAbsolutePath)
          println(s"Copying ${name} resource file://${tmpDir}/* to ${driverSettings.location}")
          val succ = fsd.copy("file://" + tmpDir + "/*", driverSettings.location, true)
          tmpDir.delete
          succ
        } else {
          println(s"Copying ${name} resource ${f} to ${driverSettings.location}")
          fsd.copy(f, driverSettings.location, true)
        }
      })

    // write list of found libjars back into config                                        
    val libJars = fsd.listFiles(driverSettings.location + "*.jar")
      .map(stat => stat.getPath.toString)
      .toList

    println("registered libjars for " + name + ": " + libJars.mkString(","))

    succ.reduceOption((a, b) => a && b).getOrElse(true)
  }
}