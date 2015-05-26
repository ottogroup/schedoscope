package com.ottogroup.bi.soda.dsl.views

import java.util.Date
import com.ottogroup.bi.soda.dsl.Field
import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.dsl.ViewDsl

trait IntervalOccurrence extends ViewDsl {
  val occurredFrom = fieldOf[String](1000)
  val occurredUntil = fieldOf[String](999)
}