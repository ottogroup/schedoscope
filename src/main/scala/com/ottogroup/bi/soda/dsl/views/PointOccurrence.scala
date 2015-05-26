package com.ottogroup.bi.soda.dsl.views

import java.util.Date
import com.ottogroup.bi.soda.dsl.Field
import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.dsl.ViewDsl

trait PointOccurrence extends ViewDsl {
  val occurredAt = fieldOf[String](1000)
}