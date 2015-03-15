package com.ottogroup.bi.soda.dsl.views

import com.ottogroup.bi.soda.dsl.ViewDsl

trait IntervalOccurrence extends ViewDsl {
  val occurredFrom = fieldOf[String](1000)
  val occurredUntil = fieldOf[String](999)
}