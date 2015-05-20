package org.schedoscope.scheduler.api

trait SchedoscopeInterface {

  def materialize(viewUrlPath: Option[String], status: Option[String], filter: Option[String], mode: Option[String]): SchedoscopeCommandStatus

  def invalidate(viewUrlPath: Option[String], status: Option[String], filter: Option[String], dependencies: Option[Boolean]): SchedoscopeCommandStatus

  def newdata(viewUrlPath: Option[String], status: Option[String], filter: Option[String]): SchedoscopeCommandStatus

  def commandStatus(commandId: String): SchedoscopeCommandStatus

  def commands(status: Option[String], filter: Option[String]): List[SchedoscopeCommandStatus]

  def views(viewUrlPath: Option[String], status: Option[String], filter: Option[String], dependencies: Option[Boolean], overview: Option[Boolean]): ViewStatusList

  def actions(status: Option[String], filter: Option[String]): ActionStatusList

  def queues(typ: Option[String], filter: Option[String]): QueueStatusList

  def shutdown() : Boolean
}