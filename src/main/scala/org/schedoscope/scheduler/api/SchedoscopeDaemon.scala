package org.schedoscope.scheduler.api

import org.apache.commons.daemon.Daemon
import org.apache.commons.daemon.DaemonContext

import org.schedoscope.scheduler.api.SchedoscopeRestService.Config

trait ApplicationLifecycle {
  def start(): Unit
  def stop(): Unit
}

abstract class AbstractApplicationDaemon extends Daemon {
  def application: ApplicationLifecycle

  def init(daemonContext: DaemonContext) {}

  def start() = application.start()

  def stop() = application.stop()

  def destroy() = application.stop()
}

class ApplicationDaemon() extends AbstractApplicationDaemon {
  def application = new SchedosopeDaemon
}

object SchedosopeDaemon extends App {
  val application = createApplication()

  def createApplication() = new ApplicationDaemon

  private[this] var cleanupAlreadyRun: Boolean = false

  def cleanup() {
    val previouslyRun = cleanupAlreadyRun
    cleanupAlreadyRun = true
    if (!previouslyRun) application.stop()
  }

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    def run() {
      cleanup()
    }
  }))

  application.start()
}

class SodaDaemon extends ApplicationLifecycle {

  def init(context: String): Unit = {}
  def init(context: DaemonContext) = {}

  def start() = SodaRestService.start(Config())

  def stop() {
    if (SodaRestService.stop())
      System.exit(0)
    else
      System.exit(1)
  }
}
