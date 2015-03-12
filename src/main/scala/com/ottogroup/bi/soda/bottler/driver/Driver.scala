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
import scala.concurrent.impl.Future
import scala.concurrent.Future
import org.joda.time.LocalDateTime
import scala.concurrent.duration.Duration
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

case class DriverException(message: String = null, cause: Throwable = null) extends RuntimeException(message, cause)

class DriverRunHandle[T <: Transformation](val driver: Driver[T], val started: LocalDateTime, val transformation: T, val stateHandle: Any, val result: Future[DriverRunState[T]])

sealed abstract class DriverRunState[T <: Transformation](val driver: Driver[T])
case class DriverRunOngoing[T <: Transformation](override val driver: Driver[T], val runHandle: DriverRunHandle[T]) extends DriverRunState[T](driver)
case class DriverRunSucceeded[T <: Transformation](override val driver: Driver[T], comment: String) extends DriverRunState[T](driver)
case class DriverRunFailed[T <: Transformation](override val driver: Driver[T], reason: String, cause: Throwable) extends DriverRunState[T](driver)

trait Driver[T <: Transformation] extends NamedDriver {
  def runTimeOut: Duration = Duration.Inf

  def killRun(run: DriverRunHandle[T]): Unit = {}

  def getDriverRunState(run: DriverRunHandle[T]): DriverRunState[T] =
    if (run.result.isCompleted)
      run.result.value.get.get
    else
      DriverRunOngoing[T](this, run)

  def run(t: T): DriverRunHandle[T]

  def runAndWait(t: T): DriverRunState[T] = Await.result(run(t).result, runTimeOut)

  def deployAll(ds: DriverSettings): Boolean = {
    val fsd = FileSystemDriver(ds)

    // clear destination
    fsd.delete(ds.location, true)
    fsd.mkdirs(ds.location)

    val succ = ds.libJars
      .map(f => {
        if (ds.unpack) {
          val tmpDir = Files.createTempDirectory("soda-" + Random.nextLong.abs.toString).toFile
          println(s"Unzipping ${name} resource ${f}")
          new ZipFile(f.replaceAll("file:", "")).extractAll(tmpDir.getAbsolutePath)
          println(s"Copying ${name} resource file://${tmpDir}/* to ${ds.location}")
          val succ = fsd.copy("file://" + tmpDir + "/*", ds.location, true)
          tmpDir.delete
          succ
        } else {
          println(s"Copying ${name} resource ${f} to ${ds.location}")
          fsd.copy(f, ds.location, true)
        }
      })

    // write list of found libjars back into config                                        
    val libJars = fsd.listFiles(ds.location + "*.jar")
      .map(stat => stat.getPath.toString)
      .toList

    println("registered libjars for " + name + ": " + libJars.mkString(","))

    succ.filter(_.isInstanceOf[DriverRunFailed[_]]).isEmpty
  }
}

trait NamedDriver {
  def name = this.getClass.getSimpleName.toLowerCase.replaceAll("driver", "").replaceAll("[^a-z]", "")
}