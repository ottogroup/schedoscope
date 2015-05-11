package org.schedoscope.dsl.views

import org.schedoscope.dsl.ViewDsl

trait Id extends ViewDsl {
  val id = fieldOf[String](Int.MaxValue)
}