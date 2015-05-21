package org.schedoscope.scheduler.api

import jline.ConsoleReader
import jline.History
import java.io.File

object SchedoscopeShell {
  def start(schedoscope: SchedoscopeInterface) {
    val ctrl = new SchedoscopeControl(schedoscope)
    val reader = new ConsoleReader()
    val history = new History()
    history.setHistoryFile(new File(System.getenv("HOME") + "/.schedoscope_history"))
    reader.setHistory(history)
    while (true) {
      try {
        val cmd = reader.readLine("schedoscope> ")
        // we have to intercept --help because otherwise jline seems to call System.exit :(
        if (cmd != null && !cmd.trim().replaceAll(";", "").isEmpty() && !cmd.matches(".*--help.*"))
          ctrl.run(cmd.split("\\s+"))
      } catch {
        case t: Throwable => println(s"ERROR: ${t.getMessage}\n\n"); t.printStackTrace()
      }
    }
  }
}