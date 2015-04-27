package com.ottogroup.bi.soda.bottler.driver

import java.nio.file.Files
import scala.Array.canBuildFrom
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.Random
import org.joda.time.LocalDateTime
import com.ottogroup.bi.soda.DriverSettings
import com.ottogroup.bi.soda.dsl.Transformation
import net.lingala.zip4j.core.ZipFile
import com.ottogroup.bi.soda.Settings

case class DriverException(message: String = null, cause: Throwable = null) extends RuntimeException(message, cause)

class DriverRunHandle[T <: Transformation](val driver: Driver[T], val started: LocalDateTime, val transformation: T, val stateHandle: Any)

sealed abstract class DriverRunState[T <: Transformation](val driver: Driver[T])
case class DriverRunOngoing[T <: Transformation](override val driver: Driver[T], val runHandle: DriverRunHandle[T]) extends DriverRunState[T](driver)
case class DriverRunSucceeded[T <: Transformation](override val driver: Driver[T], comment: String) extends DriverRunState[T](driver)
case class DriverRunFailed[T <: Transformation](override val driver: Driver[T], reason: String, cause: Throwable) extends DriverRunState[T](driver)

trait Driver[T <: Transformation]  {
  def transformationName: String

  def runTimeOut: Duration = Settings().getDriverSettings(transformationName).timeout

  def killRun(run: DriverRunHandle[T]): Unit = {}

  def getDriverRunState(run: DriverRunHandle[T]): DriverRunState[T] = {
    val runState = run.stateHandle.asInstanceOf[Future[DriverRunState[T]]]
    if (runState.isCompleted)
      runState.value.get.get
    else
      DriverRunOngoing[T](this, run)
  }

  def run(t: T): DriverRunHandle[T]

  def runAndWait(t: T): DriverRunState[T] = Await.result(run(t).stateHandle.asInstanceOf[Future[DriverRunState[T]]], runTimeOut)

  def deployAll(ds: DriverSettings): Boolean = {
    val fsd = FileSystemDriver(ds)

    // clear destination
    fsd.delete(ds.location, true)
    fsd.mkdirs(ds.location)

    val succ = ds.libJars
      .map(f => {
        if (ds.unpack) {
          val tmpDir = Files.createTempDirectory("soda-" + Random.nextLong.abs.toString).toFile
          new ZipFile(f.replaceAll("file:", "")).extractAll(tmpDir.getAbsolutePath)
          val succ = fsd.copy("file://" + tmpDir + "/*", ds.location, true)
          tmpDir.delete
          succ
        } else {
          fsd.copy(f, ds.location, true)
        }
      })

    succ.filter(_.isInstanceOf[DriverRunFailed[_]]).isEmpty
  }
}