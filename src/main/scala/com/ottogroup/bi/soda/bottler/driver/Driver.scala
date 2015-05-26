package com.ottogroup.bi.soda.bottler.driver

import com.ottogroup.bi.soda.dsl.Transformation

trait Driver {
  // non-blocking
  def run(t: Transformation): String
  // blocking
  def runAndWait(t: Transformation): Boolean
}