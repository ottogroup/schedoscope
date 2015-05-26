package com.ottogroup.bi.soda.dsl

import scala.collection.mutable.HashMap

abstract class Transformation {

  def configureWith(c: Map[String, Any]) = {
    configuration ++= c
    this
  }

  val configuration = HashMap[String, Any]()

}

case class NoOp() extends Transformation
