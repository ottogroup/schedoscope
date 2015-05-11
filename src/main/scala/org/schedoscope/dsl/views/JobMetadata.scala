package org.schedoscope.dsl.views

import java.util.Date

import org.schedoscope.dsl.ViewDsl

trait JobMetadata extends ViewDsl {
  val createdAt = fieldOf[Date](1)
  val createdBy = fieldOf[String](0)
}