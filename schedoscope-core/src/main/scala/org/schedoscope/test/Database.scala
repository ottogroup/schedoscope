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
package org.schedoscope.test

import java.sql.{Connection, ResultSet, Statement}

import org.schedoscope.dsl.{FieldLike, View}
import org.schedoscope.schema.ddl.HiveQl

import scala.collection.mutable.{HashMap, ListBuffer}

class Database(conn: Connection, url: String) {

  def selectForViewByQuery(v: View, query: String, orderByField: Option[FieldLike[_]]): List[Map[String, Any]] = {
    val res = ListBuffer[Map[String, Any]]()
    var statement: Statement = null
    var rs: ResultSet = null

    try {
      statement = conn.createStatement()
      rs = statement.executeQuery(query)

      while (rs.next()) {
        val row = HashMap[String, Any]()
        v.fields.view.zipWithIndex.foreach(f => {
          row.put(f._1.n, ViewSerDe.deserializeField(f._1.t, rs.getString(f._2 + 1)))
        })
        res.append(row.toMap)
      }
    }
    finally {
      if (rs != null) try {
        rs.close()
      } catch {
        case _: Throwable =>
      }

      if (statement != null) try {
        statement.close()
      } catch {
        case _: Throwable =>
      }
    }

    orderByField match {
      case Some(f) => res.sortBy {
        _ (f.n) match {
          case null => ""
          case other => other.toString
        }
      } toList
      case None => res.toList
    }
  }

  def selectView(v: View, orderByField: Option[FieldLike[_]]): List[Map[String, Any]] =
    selectForViewByQuery(v, HiveQl.selectAll(v), orderByField)

}