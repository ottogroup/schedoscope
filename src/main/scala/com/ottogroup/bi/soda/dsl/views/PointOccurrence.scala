package com.ottogroup.bi.soda.dsl.views

import com.ottogroup.bi.soda.dsl.ViewDsl

trait PointOccurrence extends ViewDsl {
  val occurredAt = fieldOf[String](1000)
}