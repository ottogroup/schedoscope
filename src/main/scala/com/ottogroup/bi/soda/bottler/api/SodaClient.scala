package com.ottogroup.bi.soda.bottler.api

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Try

import com.ottogroup.bi.soda.dsl.View
import spray.routing.SimpleRoutingApp

object SodaRestClient {

  var host = "localhost"
  var port = Settings().port

  import SodaJsonProtocol._

  implicit val system = ActorSystem("soda-spray-client")
  import system.dispatcher // execution context for futures below
  implicit val timeout = Timeout(10.days)
  val log = Logging(system, getClass)

  def get[T](q: String): Future[T] = {
    val pipeline = q match {
      case u: String if u.startsWith("/listactions/") => sendReceive ~> unmarshal[ProcList]
      case u: String if u.startsWith("/listviews/") => sendReceive ~> unmarshal[ViewList]
      case u: String if u.startsWith("/materialize/") => sendReceive ~> unmarshal[ViewStat]
      case _ => throw new RuntimeException("Unsupported query: " + q)
    }
    println("Calling Soda API URL: " + url(q))
    pipeline(Get(url(q))).asInstanceOf[Future[T]]
  }

  private def url(u: String) = {
    s"http://${host}:${port}${u}"
  }

  def close() {
    system.shutdown()
  }
}

/**
 * @author dbenz
 */
object SodaClient {

  def listActions = Await.result(SodaRestClient.get[ProcList]("/listactions/any"), 20.seconds)

  def listViews = Await.result(SodaRestClient.get[ViewList]("/listviews/any"), 20.seconds)

  def materialize(env: String, db: String, view: String, params: String) = {
    val viewUrlPath = s"${env}/${db}/${view}/${params}"
    Await.result(SodaRestClient.get[ViewStat](s"/materialize/${viewUrlPath}"), 10.days)
  }

  def close() {
    SodaRestClient.close()
  }
}

object SodaControl {
  
  val log = Logging(SodaRestClient.system, getClass)
  
  object Action extends Enumeration {
    val LISTVIEWS, LISTACTIONS, MATERIALIZE = Value
  }
  import Action._

  case class Config(action: Option[Action.Value] = None, environment: String = "dev", database: String = "", view: String = "", parameters: String = "", status: Option[String] = None)

  val parser = new scopt.OptionParser[Config]("soda-control") {
    override def showUsageOnError = true
    head("soda-control", "0.0.1")
    help("help") text ("print usage")
    cmd("listviews") action { (_, c) => c.copy(action = Some(LISTVIEWS)) } text ("lists all view actors, along with their status") children (
      opt[String]('s', "status") action { (x, c) => c.copy(status = Some(x)) } optional () valueName ("<status>") text ("filter views by their status (e.g. 'transforming')"))
    cmd("listactions") action { (_, c) => c.copy(action = Some(LISTACTIONS)) } text ("list status of action actors") children ()
    cmd("materialize") action { (_, c) => c.copy(action = Some(MATERIALIZE)) } text ("materialize view(s)") children (
      opt[String]('e', "environment") action { (x, c) => c.copy(environment = x) } required () valueName ("<env>") text ("environment (e.g. 'dev')"),
      opt[String]('d', "package") action { (x, c) => c.copy(database = x) } required()  valueName ("<db>") text ("database (e.g. 'my.company')"),
      opt[String]('v', "view") action { (x, c) => c.copy(view = x) } required() valueName ("<view>") optional () text ("view (e.g. 'customers'). If no view is given, all views from the database are loaded."),
      opt[String]('p', "parameters") action { (x, c) => c.copy(parameters = x) } required () valueName ("<parameters>") text ("view parameter specification (e.g. 'e(shop1,shop2)/rymd(20140101-20140107)"))
    checkConfig { c =>
      {
        if (!c.action.isDefined) failure("A command is required")
        else if (c.action.get.equals("materialize") && Try(ViewUrlParser.parse(c.environment, c.parameters)).isFailure) failure("Cannot parse view parameters")
        else success
      }
    }
  }

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) => {
        println("Starting " + config.action.get.toString + " ...")
        val res = config.action.get match {
          case LISTACTIONS => {
            SodaClient.listActions
          }
          case LISTVIEWS => {
            SodaClient.listViews
          }
          case MATERIALIZE => {
             SodaClient.materialize(config.environment, config.database, config.view, config.parameters)
          }
          case _ => {
            println("Unsupported Action: " + config.action.get.toString)
          }
        }
        println("\nRESULTS\n=======")
        println(CliFormat.serialize(res))
        SodaClient.close()
      }
      case None => // usage information has already been displayed
    }
  }

}



