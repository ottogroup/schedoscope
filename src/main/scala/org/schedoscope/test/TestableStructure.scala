package org.schedoscope.test

import org.schedoscope.dsl.FieldLike
import org.schedoscope.dsl.Structure

import com.ottogroup.bi.soda.dsl._

trait testStruct extends Structure with values {
  def filledBy(vals: Any) {
    set(vals.asInstanceOf[Array[(FieldLike[_], Any)]]: _*)
  }
}