package com.ottogroup.bi.soda.bottler.api

import scala.concurrent.Future
import com.bethecoder.ascii_table.ASCIITable

/**
 * @author dev_dbenz
 */
object show {
  def processlist() {
    CliFormat.serialize(SodaClient.listActions)
  }
}

object CliFormat {
  def serialize(o: Any): String = {
    val sb = new StringBuilder()
    o match {
      case pl: ProcList => {        
        sb.append(s"Running: ${pl.running}\n")
        sb.append(s"Idle: ${pl.idle}\n")
        sb.append(s"Queued: ${pl.queued}\n")
        sb.append(s"Details\n")
        val header = Array("STATUS","TYP","STARTED", "TRANSFORMATION")
        val running = pl.processes.map(p => Array(p.status, p.typ, p.start, p.transformation)).toArray
        val queued =  pl.queues.flatMap(q => q._2.map( e => Array("queued", q._1, "no", e))).toArray
        sb.append(ASCIITable.getInstance.getTable(header, running ++ queued))        
      }
      case vl: ViewList => {
        sb.append(vl.overview.map(el => s"${el._1}: ${el._2}\n").mkString("\n"))
        sb.append(s"Details:\n")
        val header = Array("VIEW", "STATUS", "PARAMETERS")
        val data = vl.details.map(d => Array(d.view, d.status, d.parameters)).toArray
        sb.append(ASCIITable.getInstance.getTable(header, data))
      }
      case vs: ViewStat => {
        sb.append(s"view: ${vs.view}\n")
        sb.append(s"status: ${vs.status}\n")
      }
      case f: Future[_] => {
        sb.append(s"submitted; isCompleted: ${f.isCompleted}\n")
      }
      case s: Seq[_] => {
        sb.append(s.map(el => serialize(el)).mkString("\n"))
      }
      case _ => sb.append(o)
    }
    sb.toString
  }

  def formatRow(l : String*): String = {
    "| " + l.mkString("\t| ") + " |\n"
  }  
}