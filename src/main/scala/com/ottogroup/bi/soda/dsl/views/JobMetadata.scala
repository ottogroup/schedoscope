package com.ottogroup.bi.soda.dsl.views

import java.util.Date

import com.ottogroup.bi.soda.dsl.ViewDsl

trait JobMetadata extends ViewDsl {
  val createdAt = fieldOf[Date](1)
  val createdBy = fieldOf[String](0)
}