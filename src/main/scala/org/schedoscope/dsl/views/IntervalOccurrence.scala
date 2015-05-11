package org.schedoscope.dsl.views

import org.schedoscope.dsl.ViewDsl

trait IntervalOccurrence extends ViewDsl {
  val occurredFrom = fieldOf[String](1000)
  val occurredUntil = fieldOf[String](999)
}