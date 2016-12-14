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
package org.schedoscope.scheduler.commandline

import com.bethecoder.ascii_table.ASCIITable
import org.schedoscope.scheduler.service._

import scala.concurrent.Future

object SchedoscopeCliFormat {
  // FIXME: a more generic parsing would be cool...

  private def formatMap(p: Option[Map[String, String]]) = {
    val result = if(p.isDefined) {
      p.get.foldLeft("") { (s: String, pair: (String, String)) =>
        s + ", " + pair._1 + "=" + pair._2 }
    } else ""
    if(result.length > 2)
      result.drop(2)
    else
      result
  }

  def serialize(o: Any): String = {
    val sb = new StringBuilder()
    o match {
      case as: TransformationStatusList => {
        if (as.transformations.size > 0) {
          val header = Array("TRANSFORMATION DRIVER", "STATUS", "STARTED", "DESC", "TARGET_VIEW", "PROPS")
          val running = as.transformations.map(p => {
            val (s, d, t): (String, String, String) =
              if (p.runStatus.isDefined) {
                (p.runStatus.get.started, p.runStatus.get.description, p.runStatus.get.targetView)
              } else {
                ("", "", "")
              }
            Array(p.actor, p.status, s, d, t, formatMap(p.properties))
          }).toArray
          sb.append(ASCIITable.getInstance.getTable(header, running))
          sb.append(s"Total: ${running.size}\n")
        }
        sb.append("\n" + as.overview.map(el => s"${el._1} : ${el._2}").mkString("\n") + "\n")
      }

      case qs: QueueStatusList => {
        if (qs.queues.flatMap(q => q._2).size > 0) {
          val header = Array("TYP", "DESC", "TARGET_VIEW", "PROPS")
          val queued = qs.queues.flatMap(q => q._2.map(e =>
            Array(q._1, e.description, e.targetView, formatMap(e.properties))
          )).toArray
          sb.append(ASCIITable.getInstance.getTable(header, queued))
          sb.append(s"Total: ${queued.size}")
        }
        sb.append("\n" + qs.overview.toSeq.sortBy(_._1).map(el => s"${el._1} : ${el._2}").mkString("\n") + "\n")
      }

      case vl: ViewStatusList => {
        if (!vl.views.isEmpty) {
          sb.append(s"Details:\n")
          val header = Array("VIEW", "STATUS", "PROPS")

          val fields = vl.views
            .filter { viewStatus => viewStatus.fields.isDefined && viewStatus.viewTableName.isDefined }
            .foldLeft(scala.collection.mutable.Map[String, List[FieldStatus]]()) {
              (map, viewStatus) => map += (viewStatus.viewTableName.get -> viewStatus.fields.get)
            }

          val data = vl.views
            .filter(!_.isTable.getOrElse(false))
            .map(d => Array(d.viewPath, d.status,
                if (d.viewTableName.isDefined && fields.get(d.viewTableName.get).isDefined) {
                  fields.get(d.viewTableName.get).get.map(fieldStatus =>
                    fieldStatus.name + "::" + fieldStatus.fieldtype).mkString(", ") +
                    ", " + formatMap(d.properties)
                } else formatMap(d.properties) )
            ).toArray

          sb.append(ASCIITable.getInstance.getTable(header, data))
          sb.append(s"Total: ${data.size}\n")
        }
        sb.append("\n" + vl.overview.map(el => s"${el._1}: ${el._2}").mkString("\n") + "\n")
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
}