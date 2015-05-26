package com.ottogroup.bi.soda.dsl.views

import com.ottogroup.bi.soda.dsl.Field
import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.dsl.ViewDsl

trait Id extends ViewDsl {
  val id = fieldOf[String](Int.MaxValue)
}