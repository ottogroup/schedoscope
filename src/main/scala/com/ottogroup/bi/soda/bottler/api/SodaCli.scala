package com.ottogroup.bi.soda.bottler.api

import scala.concurrent.Future

/**
 * @author dev_dbenz
 */
object show {
  def processlist() {
    CliFormat.serialize(SodaClient.listActions)
  }
}

object CliFormat {
    def serialize(o : Any) : String = {
    val sb = new StringBuilder()
    o match {
      case pl: ProcList => {
        sb.append(s"Running: ${pl.running}\n")
        sb.append(s"Idle: ${pl.idle}\n")
        sb.append(s"Details:\n")
        sb.append(s"|${pad("status", 10)}|${pad("typ", 8)}|${pad("started", 35)}|${pad("transformation", 50)}|\n")
        sb.append(pl.processes.map( p => s"|${pad(p.status,10)}|${pad(p.typ, 8)}|${pad(p.start, 35)}|${pad(p.transformation,50)}|").mkString("\n"))
      }
      case vl: ViewList => {
        sb.append(vl.overview.map(el => s"${el._1}: ${el._2}\n").mkString("\n"))
        sb.append(s"Details:\n")
        sb.append(s"|${pad("view", 35)}|${pad("status", 15)}|${pad("parameters", 50)}|\n")
        sb.append(vl.details.map(d => s"|${pad(d.view,35)}|${pad(d.status,15)}|${pad(d.parameters,50)}|").mkString("\n"))
      }
      case vs : ViewStat => {
        sb.append(s"view: ${vs.view}\n")
        sb.append(s"status: ${vs.status}\n")
      }
      case f : Future[_] => {
        sb.append(s"submitted; isCompleted: ${f.isCompleted}\n")
      }
      case s : Seq[_] => {
        sb.append( s.map(el => serialize(el)).mkString("\n") )
      }
      case _ => sb.append(o)
    }
    sb.toString
  }

  def pad(s: String, l: Int): String = {
    " " + s.replaceAll("\\n", "").replaceAll("\\t", "").padTo(l, " ").mkString + " "
  }
}