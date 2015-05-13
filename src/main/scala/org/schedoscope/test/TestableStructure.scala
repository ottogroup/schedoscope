package org.schedoscope.test

import org.schedoscope.dsl._

trait testStruct extends Structure with values {
  def filledBy(vals: Any) {
    set(vals.asInstanceOf[Array[(FieldLike[_], Any)]]: _*)
  }
}