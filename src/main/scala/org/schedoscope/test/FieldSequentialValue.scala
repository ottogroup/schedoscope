package org.schedoscope.test

import java.text.SimpleDateFormat
import java.util.Date

import org.schedoscope.dsl.FieldLike
import org.schedoscope.dsl.Structure

object FieldSequentialValue {
  def get(f: FieldLike[_], i: Int, p: String): Any = {
    if (f.t == manifest[Int])
      i
    else if (f.t == manifest[Long])
      i.toLong
    else if (f.t == manifest[Byte])
      i.toByte
    else if (f.t == manifest[Boolean])
      i % 2 == 0
    else if (f.t == manifest[Double])
      i.toDouble
    else if (f.t == manifest[Float])
      i.toFloat
    else if (f.t == manifest[Date])
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").format(new Date(i * 1000L))
    else if (f.t == manifest[String])
      f.n + "-" + p.format(i)
    else if (classOf[Structure].isAssignableFrom(f.t.runtimeClass)) {
      f.t.runtimeClass.newInstance().asInstanceOf[Structure].fields.map(sf => (sf.n, get(sf, i, p))).toMap
    } else if (f.t.runtimeClass == classOf[List[_]]) {
      List()
    } else if (f.t.runtimeClass == classOf[Map[_, _]])
      Map()
    else
      throw new RuntimeException("Cannot generate random values for: " + f.n + ", type is: " + f.t)
  }
}