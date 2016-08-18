package org.schedoscope.scheduler.driver

import java.io.{File, FileWriter}

import org.joda.time.LocalDateTime
import org.schedoscope.conf.DriverSettings
import org.schedoscope.dsl.transformations.ShellTransformation
import org.schedoscope.test.resources.TestResources
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.language.reflectiveCalls
import scala.sys.process._

/**
  * Driver for executing shell transformations.
  */
class ShellDriver(val driverRunCompletionHandlerClassNames: List[String]) extends DriverOnBlockingApi[ShellTransformation] {

  val log = LoggerFactory.getLogger(classOf[ShellDriver])

  def transformationName = "shell"

  /**
    * Construct a future-based driver run handle
    */
  def run(t: ShellTransformation): DriverRunHandle[ShellTransformation] =
    new DriverRunHandle(this, new LocalDateTime(), t, Future {
      doRun(t)
    })

  /**
    * Actually run the shell transformation.
    */
  def doRun(t: ShellTransformation): DriverRunState[ShellTransformation] = {
    val stdout = new StringBuilder
    try {
      val returnCode = if (t.scriptFile != "")
        Process(Seq(t.shell, t.scriptFile), None, t.configuration.toSeq.asInstanceOf[Seq[(String, String)]]: _*).!(ProcessLogger(stdout append _, log.error(_)))
      else {
        val file = File.createTempFile("_schedoscope", ".sh")

        val writer = new FileWriter(file)
        try {
          writer.write(s"#!${t.shell}\n")
          t.script.foreach {
            writer.write(_)
          }
        } finally {
          writer.close()
        }

        scala.compat.Platform.collectGarbage() // JVM Windows related bug workaround JDK-4715154
        file.deleteOnExit()
        Process(Seq(t.shell, file.getAbsolutePath), None, t.configuration.toSeq.asInstanceOf[Seq[(String, String)]]: _*).!(ProcessLogger(stdout append _, log.error(_)))
      }
      if (returnCode == 0)
        DriverRunSucceeded[ShellTransformation](this, "Shell script finished")
      else
        DriverRunFailed[ShellTransformation](this, s"Shell script returned errorcode ${returnCode}", RetryableDriverException(s"Failed shell script, status ${returnCode}"))
    } catch {
      case e: Throwable => DriverRunFailed[ShellTransformation](this, s"Shell script execution resulted in exception", e)
    }
  }
}

/**
  * Factory methods for shell transformations.
  */
object ShellDriver extends DriverCompanionObject[ShellTransformation] {

  def apply(ds: DriverSettings) = new ShellDriver(ds.driverRunCompletionHandlers)

  def apply(ds: DriverSettings, testResources: TestResources) =
    new ShellDriver(List("org.schedoscope.test.resources.TestDriverRunCompletionHandler"))
}
