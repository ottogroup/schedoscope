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

import java.sql.Connection
import scala.collection.mutable.ListBuffer
import org.schedoscope.dsl.View
import org.schedoscope.dsl.Field
import org.schedoscope.schema.ddl.HiveQl
import java.util.Date
import scala.collection.generic.MutableMapFactory
import scala.collection.mutable.HashMap
import org.schedoscope.dsl.FieldLike
import java.sql.DriverManager

class Database(conn: Connection, url: String) {

  def selectForViewByQuery(v: View, q: String, o: FieldLike[_]): List[Map[String, Any]] = {
    val res = ListBuffer[Map[String, Any]]()
    val rs = conn.createStatement().executeQuery(q)
    while (rs.next()) {
      val row = HashMap[String, Any]()
      v.fields.view.zipWithIndex.foreach(f => {
        row.put(f._1.n, ViewSerDe.deserializeField(f._1.t, rs.getString(f._2 + 1)))
      })
      res.append(row.toMap)
    }
    rs.close()
    if (o != null)
      res.toList.sortBy(_(o.n).toString())
    else
      res.toList
  }

  def selectForView(v: View, o: FieldLike[_]): List[Map[String, Any]] = {
    selectForViewByQuery(v, HiveQl.selectAll(v), o)
  }

}