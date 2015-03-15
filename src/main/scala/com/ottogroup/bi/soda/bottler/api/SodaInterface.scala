package com.ottogroup.bi.soda.bottler.api

import scala.concurrent.ExecutionContext
import akka.util.Timeout
import com.ottogroup.bi.soda.bottler.ViewSuperVisor
import com.ottogroup.bi.soda.bottler.ActionsRouterActor
import com.ottogroup.bi.soda.bottler.SchemaActor
import scala.concurrent.duration._
import spray.json._
import org.joda.time.LocalDateTime
import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.dsl.Transformation
import com.ottogroup.bi.soda.bottler.ViewStatusResponse
import com.ottogroup.bi.soda.bottler.ActionStatusResponse


class SodaServerInterface extends SodaInterface  {
  
//  val settings = Settings()
// 
//  implicit val ec = ExecutionContext.global
//  implicit val timeout = Timeout(3 days) // needed for `?` below
//
//  val supervisor = settings.system.actorOf(ViewSuperVisor.props(settings), "supervisor")
//  val scheduleActor = settings.system.actorOf(ActionsRouterActor.props(settings.hadoopConf), "actions")
//  val schemaActor = settings.system.actorOf(SchemaActor.props(settings.jdbcUrl, settings.metastoreUri, settings.kerberosPrincipal), "schemaActor")
  
  def materialize(viewUrlPath: String) = {null}
  
  def commandStatus(commandId: String) = {null}
  
  def commands(status: String) = {null}
  
  def views(viewUrlPath : String, status: String)  = {null}  
  
  def invalidate(viewUrlPath: String)  = {null}
  
  def newdata(viewUrlPath: String)  = {null}
  
  def actions(status: String)  = {null}  
  
  def dependencies(viewUrlPath: String, recursive: Boolean)  = {null}  
}




trait SodaInterface {
  
  def materialize(viewUrlPath: String) : SodaCommandStatus
  
  def commandStatus(commandId: String) : SodaCommandStatus
  
  def commands(status: String) : List[SodaCommandStatus]
  
  def views(viewUrlPath : String, status: String) : ViewStatusList  
  
  def invalidate(viewUrlPath: String) : SodaCommandStatus
  
  def newdata(viewUrlPath: String) : SodaCommandStatus
  
  def actions(status: String) : ActionStatusList  
  
  def dependencies(viewUrlPath: String, recursive: Boolean): DependencyGraph
  
}