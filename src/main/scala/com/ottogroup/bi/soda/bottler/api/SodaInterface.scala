package com.ottogroup.bi.soda.bottler.api

trait SodaInterface {

  def materialize(viewUrlPath: String): SodaCommandStatus

  def invalidate(viewUrlPath: String): SodaCommandStatus

  def newdata(viewUrlPath: String): SodaCommandStatus

  def commandStatus(commandId: String): SodaCommandStatus

  def commands(status: Option[String]): List[SodaCommandStatus]

  def views(viewUrlPath: Option[String], status: Option[String], withDependencies: Boolean = false): ViewStatusList

  def actions(status: Option[String]): ActionStatusList

  def shutdown()
}