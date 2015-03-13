package com.ottogroup.bi.soda.test

import scala.collection.mutable.HashMap

import com.ottogroup.bi.soda.dsl.FieldLike
import com.ottogroup.bi.soda.dsl.Structure

trait FillableStructure extends Structure with values {}

trait values extends Structure {

  def idPattern = "%02d"

  override def namingBase = this.getClass.getSuperclass.getSimpleName()
  val fs = HashMap[String, Any]()
  def set(value: (FieldLike[_], Any)*) {
    value.foreach(el => fs.put(el._1.n, el._2))
    fields.filter(f => !fs.contains(f.n)).map(f => fs.put(f.n, FieldSequentialValue.get(f, 0, idPattern)))
  }
  def get() = fs.toArray

  def get[T](f: FieldLike[T]): T = {
    fs.get(f.n).get.asInstanceOf[T]
  }

  def get(s: String): String = {
    fs.get(s).get.toString
  }

  override def equals(o: Any) = o match {
    case that: values => this.fs.equals(that.fs)
    case _ => false
  }

  override def toString = s"Structure(${fs.mkString(",")})"
}