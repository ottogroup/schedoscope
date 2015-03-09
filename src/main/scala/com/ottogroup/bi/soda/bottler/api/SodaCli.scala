package com.ottogroup.bi.soda.bottler.api

/**
 * @author dev_dbenz
 */
object show {
  def processlist() {
    CliFormat.serialize(SodaClient.listActions)
  }
}

object CliFormat {
    def serialize(o : Any) = {
    val sb = new StringBuilder()
    o match {
      case pl : ProcList => {
        sb.append(s"Running: ${pl.running}\n")
        sb.append(s"Idle: ${pl.idle}\n")
        sb.append(s"Details:\n")
        sb.append(s"|${pad("status", 10)}|${pad("typ", 8)}|${pad("started", 35)}|${pad("job", 50)}|\n")
        sb.append(pl.processes.map( p => s"|${pad(p.status,10)}|${pad(p.typ, 8)}|${pad(p.start, 35)}|${pad(p.job,50)}|").mkString("\n"))
      }
    }
    o.toString
  }
  
  def pad(s : String, l: Int) : String = {
    " " + s.replaceAll("\\n", "").replaceAll("\\t", "").padTo(l, " ").mkString + " "
  }
}