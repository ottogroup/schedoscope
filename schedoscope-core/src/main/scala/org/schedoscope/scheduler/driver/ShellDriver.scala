package org.schedoscope.scheduler.driver

import org.schedoscope.dsl.transformations.ShellTransformation
import org.schedoscope.DriverSettings
import org.schedoscope.Settings
import org.joda.time.LocalDateTime
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.concurrent.future
import scala.collection.JavaConversions.mapAsJavaMap
import java.lang.InterruptedException
import java.io.File
import java.io.FileWriter
import scala.sys.process._
import org.slf4j.LoggerFactory

class ShellDriver(val driverRunCompletionHandlerClassNames: List[String]) extends Driver[ShellTransformation] {
  override def transformationName = "shell"

  implicit val executionContext = Settings().system.dispatchers.lookup("akka.actor.future-driver-dispatcher")
  val log = LoggerFactory.getLogger(classOf[ShellDriver])

  def run(t: ShellTransformation): DriverRunHandle[ShellTransformation] =
    new DriverRunHandle(this, new LocalDateTime(), t, future {
      doRun(t)
    })

  def doRun(t: ShellTransformation): DriverRunState[ShellTransformation] = {
    try {
      val returnCode = if (t.scriptFile != "")
        Process(Seq(t.shell, t.scriptFile), None, t.env.toSeq: _*).!(ProcessLogger(line => log.error(line)))
      else {
        val file = File.createTempFile("_schedoscope", ".sh")
        using(new FileWriter(file))(writer => { writer.write(s"#!${t.shell}\n"); t.script.foreach(line => writer.write(line)) })
        scala.compat.Platform.collectGarbage() // JVM Windows related bug workaround JDK-4715154
        file.deleteOnExit()
        Process(Seq(t.shell, file.getAbsolutePath), None, t.env.toSeq: _*).!(ProcessLogger(line => log.error(line)))
      }
      if (returnCode == 0)
        DriverRunSucceeded[ShellTransformation](this, "Shell script finished")
      else
        DriverRunFailed[ShellTransformation](this, s"Shell script returned errorcode ${returnCode}", DriverException(s"Failed shell script, status ${returnCode}"))
    } catch {
      case e: InterruptedException => DriverRunFailed[ShellTransformation](this, s"Shell script got interrupted", e)
    }
  }

  def writeStringToFile(file: File, data: String, appending: Boolean = false) =
    using(new FileWriter(file, appending))(_.write(data))

  def using[A <: { def close(): Unit }, B](resource: A)(f: A => B): B =
    try f(resource) finally resource.close()

}
object ShellDriver {
  def apply(ds: DriverSettings) = new ShellDriver(ds.driverRunCompletionHandlers)
}
