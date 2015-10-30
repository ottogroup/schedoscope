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

import scala.collection.mutable.HashMap
import org.schedoscope.dsl.FieldLike
import org.schedoscope.dsl.Structure

trait FillableStructure extends Structure with values {}

/**
 * Extends a structure in so that it actually can hold values. Values are stored in a
 * HashMap.
 *
 */
trait values extends Structure {

  def idPattern = "%02d"

  override def namingBase = this.getClass.getSuperclass.getSimpleName()
   
  val fs = HashMap[String, Any]()
  
  /** sets values in a structure by specified by fieldlike
   * @param value
   */
  def set(value: (FieldLike[_], Any)*) {
    value.foreach(el => fs.put(el._1.n, el._2))
    fields.filter(f => !fs.contains(f.n)).map(f => fs.put(f.n, FieldSequentialValue.get(f, 0, idPattern)))
  } 
  
  /**
   * sets values in a structure by specified by name
   * @param key
   * @param value
   */
  def setByName(key:String,value:Any) {
    fs.put(key,value)
    fields.filter(f => !fs.contains(f.n)).map(f => fs.put(f.n, FieldSequentialValue.get(f, 0, idPattern)))
  }
  
  /**
   * returns the structure as an array of Tuples (fielname, value)
   * 
   * @return
   */
  def get() = fs.toArray

  /**
   * Returns a specific field of this structure
   * 
   * @param f
   * @return
   */
  def get[T](f: FieldLike[T]): T = {
    fs.get(f.n).get.asInstanceOf[T]
  }

  /**
   * Returns a specific field of this structure by name
   * @param s
   * @return
   */
  def get(s: String): String = {
    fs.get(s).get.toString
  }

  override def equals(o: Any) = o match {
    case that: values => this.fs.equals(that.fs)
    case _            => false
  }

  override def toString = s"Structure(${fs.mkString(",")})"
}