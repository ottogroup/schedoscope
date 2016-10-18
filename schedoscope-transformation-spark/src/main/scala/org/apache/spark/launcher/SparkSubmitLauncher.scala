package org.apache.spark.launcher

import java.io.IOException
import java.io.File
import java.net.URLClassLoader
import java.util.concurrent.atomic.AtomicInteger

import org.apache.commons.io.FileUtils
import org.apache.spark.deploy.SparkSubmit

import scala.collection.JavaConversions._
import org.apache.spark.launcher.SparkAppHandle.Listener
import org.schedoscope.dsl.transformations.SparkTransformation._


/**
  * We sadly have to rig SparkLauncher. Instead of launching the spark-submit shell script, it should
  * launch org.apache.spark.deploy.SparkSubmit via a subprocess directly.
  *
  * One of the reasons why we want this is that we do not have the shell script in our test environment (SparkSubmit we have, however);
  * also, the script just calls org.apache.spark.deploy.SparkSubmit.main by itself, making this an unnecessary detour.
  *
  * SparkSubmitCommandBuilder actually offers a method for preparing a direct call for launching SparkSubmit. However,
  * SparkLauncher does not call this method. Even worse, the method createBuilder() where it creates its ProcessBuilder
  * object is private so we cannot just override it.
  *
  * Sorry for this hack.
  */
class SparkSubmitLauncher extends SparkLauncher {

  val COUNTER: AtomicInteger = new AtomicInteger()

  def setChildEnv(key: String, value: String): SparkSubmitLauncher = {
    CommandBuilderUtils.checkNotNull(key, "childEnv key")
    CommandBuilderUtils.checkNotNull(value, "childEnv value")
    builder.childEnv.put(key, value)
    this
  }

  def setScalaVersion(scalaVersion: String): SparkSubmitLauncher = {
    CommandBuilderUtils.checkNotNull(scalaVersion, "scalaVersion")
    builder.childEnv.put("SPARK_SCALA_VERSION", scalaVersion)
    this
  }

  def setAssemblyPath(assemblyPath: String): SparkSubmitLauncher = {
    CommandBuilderUtils.checkNotNull(assemblyPath, "assemblyPath")
    builder.childEnv.put("_SPARK_ASSEMBLY", assemblyPath)
    this
  }

  def addLocalClasspath(): SparkSubmitLauncher = {
    val cl = ClassLoader.getSystemClassLoader().asInstanceOf[URLClassLoader]
    builder.childEnv.put("SPARK_CLASSPATH", cl.getURLs.map(_.getFile).toList.mkString(File.pathSeparator))
    this
  }

  def createFakeLibManaged(): SparkSubmitLauncher = {
    val dir = new File(builder.getSparkHome, "lib_managed" + File.separator + "jars")
    FileUtils.forceMkdir(dir)
    this
  }

  def setLocalTestMode(): SparkSubmitLauncher = {
    setSparkHome("target")
    setAssemblyPath(jarOf(SparkSubmit))
    addLocalClasspath()
    createFakeLibManaged()
    setScalaVersion("2.11")
    setMaster("local")
    this
  }

  @throws[IOException]
  override def startApplication(listeners: Listener*) = {

    //
    // create LauncherServer app handle
    //

    val handle = LauncherServer.newAppHandle()

    for (l <- listeners)
      handle.addListener(l)


    //
    // Set logger name
    //

    val childLoggerName: String =
      if (builder.getEffectiveConfig.get("spark.launcher.childProcLoggerName") != null)
        builder.getEffectiveConfig.get("spark.launcher.childProcLoggerName")
      else if (builder.appName != null)
        builder.appName
      else if (builder.mainClass != null) {
        val endOfPackage = builder.mainClass.lastIndexOf(".")
        if (endOfPackage >= 0 && endOfPackage < builder.mainClass.length() - 1)
          builder.mainClass.substring(endOfPackage + 1, builder.mainClass.length())
        else
          builder.mainClass
      } else if (builder.appResource != null)
        new File(builder.appResource).getName
      else
        s"${COUNTER.incrementAndGet()}"

    val fullLoggerName = s"${getClass.getPackage.getName}.app.$childLoggerName"

    //
    // Build spark submit call
    //

    val sparkSubmitCall = builder.buildCommand(Map[String, String]())

    //
    // Create subprocess
    //

    val process = new ProcessBuilder(sparkSubmitCall)

    process.redirectErrorStream(true)

    for ((k, v) <- builder.childEnv)
      process.environment().put(k, v)

    process.environment().put("_SPARK_LAUNCHER_PORT", String.valueOf(LauncherServer.getServerInstance.getPort))
    process.environment().put("_SPARK_LAUNCHER_SECRET", handle.getSecret)


    //
    // Start process and return LauncherServer app handle to caller
    //

    try {
      handle.setChildProc(process.start(), fullLoggerName)
      handle
    } catch {
      case e: IOException =>
        handle.kill()
        throw e
    }
  }
}
