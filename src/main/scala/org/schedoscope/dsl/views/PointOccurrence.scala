package org.schedoscope.dsl.views

import org.schedoscope.dsl.ViewDsl

trait PointOccurrence extends ViewDsl {
  val occurredAt = fieldOf[String](1000)
}