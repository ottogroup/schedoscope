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

import java.text.SimpleDateFormat
import java.util.Date
import java.util.ArrayList
import java.util.HashMap

import scala.collection.JavaConversions._

import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.map.`type`.TypeFactory
import org.schedoscope.dsl.storageformats._
import org.schedoscope.dsl.{Structure, View}
import org.slf4j.LoggerFactory

/**
  * Helper for serialization/deserialization of hive data types
  *
  */
object ViewSerDe {
  val logger = LoggerFactory.getLogger("gna")

  /**
    * Recursively convert nested Java collections to Scala collections
    *
    * @param j the java collection to convert
    * @return
    */
  private def toScala(j: Any): Any = j match {
    case jum: java.util.Map[_, _] => jum.map { case (k, v) => (toScala(k), toScala(v)) }.toList.toMap
    case jal: java.util.List[_] => jal.map(toScala).toList
    case _ => j
  }

  /**
    * Escape data before writing it to hive.
    *
    * @param v
    * @return
    */
  def serialize(v: View with WritableView): String = {
    v.storageFormat match {
      case tf: TextFile => {
        val fterm = if (tf.fieldTerminator == null) "\t" else tf.fieldTerminator.replaceAll("\\\\t", "\t")
        val lterm = if (tf.lineTerminator == null) "\n" else tf.lineTerminator.replaceAll("\\\\n", "\n")
        v.rowData.map(row =>
          v.fields.map(cell => {
            serializeCell(row(cell.n), false, tf)
          }).mkString(fterm))
          .mkString(lterm)
      }
      case _ => throw new RuntimeException("Can only serialize views stored as textfile")
    }
  }

  /**
    * Converts the string representation of a Field to a Value according to the type information
    * provided by schedoscope
    *
    */
  def deserializeField[T](t: Manifest[T], v: String): Any = {
    if (v == null || "null".equals(v)) {
      return v
    }
    if (t == manifest[Int])
      v.asInstanceOf[String].toInt
    else if (t == manifest[Long])
      v.asInstanceOf[String].toLong
    else if (t == manifest[Byte])
      v.asInstanceOf[String].toByte
    else if (t == manifest[Boolean])
      v.asInstanceOf[String].toBoolean
    else if (t == manifest[Double])
      v.asInstanceOf[String].toDouble
    else if (t == manifest[Float])
      v.asInstanceOf[String].toFloat
    else if (t == manifest[String])
      v.asInstanceOf[String]
    else if (t == manifest[Date])
      v.asInstanceOf[String] // TODO: parse date?
    else if (classOf[Structure].isAssignableFrom(t.runtimeClass)) {
      val res: HashMap[String, _] = new ObjectMapper().readValue(v.toString, TypeFactory.mapType(classOf[HashMap[_, _]], classOf[String], classOf[Any]))
      toScala(res)
    } else if (t.runtimeClass == classOf[List[_]]) {
      val res: ArrayList[_] = new ObjectMapper().readValue(v.toString, TypeFactory.collectionType(classOf[ArrayList[_]], classOf[Any]))
      toScala(res)
    } else if (t.runtimeClass == classOf[Map[_, _]]) {
      val res: HashMap[String, _] = new ObjectMapper().readValue(v.toString, TypeFactory.mapType(classOf[java.util.HashMap[_, _]], classOf[String], classOf[Any]))
      toScala(res)
    } else throw new RuntimeException("Could not deserialize field of type " + t + " with value " + v)
  }

  private def serializeCell(c: Any, inList: Boolean, format: TextFile): String = {
    c match {
      case null => {
        "\\N"
      }
      case s: Structure with values => {
        s.fields.map(f => serializeCell(s.fs(f.n), false, format)).mkString(if (inList) format.mapKeyTerminator else format.collectionItemTerminator)
      }
      case l: List[_] => {
        l.map(e => serializeCell(e, true, format)).mkString(format.collectionItemTerminator)
      }
      case m: Map[_, _] => {
        m.map(e => serializeCell(e._1, false, format) + format.mapKeyTerminator + serializeCell(e._2, false, format)).mkString(format.collectionItemTerminator)
      }
      case d: Date => new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").format(d)
      case _ => {
        c.toString
      }
    }
  }

}