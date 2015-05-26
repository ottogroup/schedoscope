package com.ottogroup.bi.soda.test

import com.ottogroup.bi.soda.dsl.Structure
import com.ottogroup.bi.soda.dsl.FieldLike
import com.ottogroup.bi.soda.dsl.View

trait testStruct extends Structure with values {
  def filledBy(vals: Any) {
    set(vals.asInstanceOf[Array[(FieldLike[_], Any)]]: _*)
  }
}