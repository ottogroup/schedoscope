package org.schedoscope.test

import java.sql.Connection
import scala.collection.mutable.ListBuffer
import com.ottogroup.bi.soda.dsl.View
import org.schedoscope.dsl.Field
import com.ottogroup.bi.soda.crate.ddl.HiveQl
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