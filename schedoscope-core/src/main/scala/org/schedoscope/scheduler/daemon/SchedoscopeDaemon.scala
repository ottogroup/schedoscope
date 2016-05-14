/**
 * Copyright 2015 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schedoscope.scheduler.daemon

import org.apache.commons.daemon.{ Daemon, DaemonContext }
import org.schedoscope.scheduler.rest.server.SchedoscopeRestService
import org.schedoscope.scheduler.rest.server.SchedoscopeRestService.Config

class SchedoscopeDaemon() extends Daemon {
  def init(daemonContext: DaemonContext) {}

  def start() = SchedoscopeRestService.start(Config())

  def stop() {
    if (SchedoscopeRestService.stop())
      System.exit(0)
    else
      System.exit(1)
  }

  def destroy() = stop()
}

object SchedoscopeDaemon extends App {
  val application = new SchedoscopeDaemon()

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

