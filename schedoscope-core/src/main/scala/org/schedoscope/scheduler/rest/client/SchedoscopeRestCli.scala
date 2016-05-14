package org.schedoscope.scheduler.rest.client

import org.schedoscope.Schedoscope
import org.schedoscope.scheduler.commandline.SchedoscopeCliCommandRunner

/**
 * A command line client to schedoscope that issues the CLI commands passed as command line parameters to the
 * REST web service and renders the results.
 */
object SchedoscopeRestCli extends App {
  val schedoscope = new SchedoscopeServiceRestClientImpl(Schedoscope.settings.host, Schedoscope.settings.port)

  new SchedoscopeCliCommandRunner(schedoscope).run(args)

  schedoscope.shutdown()
  System.exit(0)
}