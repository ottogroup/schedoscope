package org.schedoscope.scheduler.service

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import akka.util.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mock.MockitoSugar
import org.schedoscope.scheduler.actors.{ViewActor, ViewManagerActor}
import org.schedoscope.scheduler.driver.{DriverRunOngoing, HiveDriver}
import org.schedoscope.scheduler.messages._
import org.schedoscope.{Schedoscope, Settings, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.schedoscope.dsl.Parameter._
import org.schedoscope.dsl.View
import org.schedoscope.dsl.transformations.HiveTransformation

import test.views.{ProductBrand, Brand}

import scala.concurrent.Await
import scala.concurrent.duration._

class SchedoscopeServiceImplSpec extends TestKit(ActorSystem("schedoscope"))
  with ImplicitSender
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll
  with MockitoSugar
  with ScalaFutures {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  /**
    * More info on API check wiki: https://github.com/ottogroup/schedoscope/wiki/Schedoscope%20HTTP%20API
    */

  // defaults
  lazy val errorMsg = "\nLittle path format guide\n========================\n\n/{package}/{view}(/{view parameter value})*\n\nView parameter value format:\n  i(aNumber)                    => an integer\n  l(aNumber)                    => a long\n  b(aNumber)                    => a byte\n  t(true)|t(false)              => a boolean\n  f(aFloat)                     => a float\n  d(aDouble)                    => a double\n  ym(yyyyMM)                    => a MonthlyParameterization\n  ymd(yyyyMMdd)                 => a DailyParameterization\n  null()                        => null\n  everything else               => a string\n\nRanges on view parameter values:\n  rym(yyyyMM-yyyyMM)            => all MonthlyParameterizations between the first (earlier) and the latter (later)\n  rymd(yyyyMMdd-yyyyMMdd)       => all DailyParameterizations between the first (earlier) and the latter (later)\n  e{constructor parameter value format}({aValue},{anotherValue})\n                                => enumerate multiple values for a given view parameter value format.\n  For instance: \n    ei(1,2,3)                   => an enumeration of integer view parameters \n    e(aString, anotherString)   => an enumeration of string view parameters \n    eymd(yyyyMM,yyyMM)          => an enumeration of MonthlyParameterizations\n    erymd(yyyyMM-yyyyMM,yyyyMM-yyyyMM) => an enumeration of MonthlyParameterization ranges\n\nQuoting:\n  Use backslashes to escape the syntax given above. The following characters need quotation: \\,(-)\n"

  // dummy data
  val year = "2014"
  val month = "01"
  val day = "01"
  val shop01 = "EC01"

  val prodBrandUrl01 = s"test.views/ProductBrand/${shop01}/${year}/${month}/${day}"
  val prodUrl01 = s"test.views/Product/${shop01}/${year}/${month}/${day}"
  val brandUrl01 = s"test.views/Brand/${shop01}"

  val productBrandView01 = ProductBrand(p(shop01), p(year), p(month), p(day))
  val brandDependency01:View = productBrandView01.dependencies.head
  val productDependency01:View = productBrandView01.dependencies(1)

  val TIMEOUT = 5 seconds
  // views Status along their lifecycle
  lazy val initStatus = "receive"
  lazy val wait4dep = "waiting"
  lazy val material = "materialized"


  trait ViewManagerActorTest {
    // required for sending msg to actors (futures)
    implicit val timeout = Timeout(TIMEOUT)

    val schemaManagerRouter = TestProbe()
    val transformationManagerActor = TestProbe()

    Schedoscope.actorSystemBuilder = () => system

    val viewManagerActor = TestActorRef(
      ViewManagerActor.props(
            Schedoscope.settings,
            transformationManagerActor.ref,
            schemaManagerRouter.ref))

    Schedoscope.viewManagerActorBuilder = () => viewManagerActor

    def initializeView(view: View): ActorRef = {
      val future = viewManagerActor ? view
      schemaManagerRouter.expectMsg(CheckOrCreateTables(List(view)))
      schemaManagerRouter.reply(SchemaActionSuccess())
      schemaManagerRouter.expectMsg(AddPartitions(List(view)))
      schemaManagerRouter.reply(TransformationMetadata(Map(view -> ("test", 1L))))

      Await.result(future, TIMEOUT)
      future.isCompleted shouldBe true
      future.value.get.isSuccess shouldBe true
      future.value.get.get.asInstanceOf[ActorRef]
    }

    def getInitializedView(view: View): ActorRef = {
      val future = viewManagerActor ? view
      Await.result(future, TIMEOUT)
      future.isCompleted shouldBe true
      future.value.get.isSuccess shouldBe true
      future.value.get.get.asInstanceOf[ActorRef]
    }

    def initializeViewWithDep(view: View, brandDependency:View, productDependency:View): ActorRef = {
      val future = viewManagerActor ? view

      schemaManagerRouter.expectMsg(CheckOrCreateTables(List(brandDependency)))
      schemaManagerRouter.reply(SchemaActionSuccess())
      schemaManagerRouter.expectMsg(CheckOrCreateTables(List(view)))
      schemaManagerRouter.reply(SchemaActionSuccess())
      schemaManagerRouter.expectMsg(CheckOrCreateTables(List(productDependency)))
      schemaManagerRouter.reply(SchemaActionSuccess())
      schemaManagerRouter.expectMsg(AddPartitions(List(brandDependency)))
      schemaManagerRouter.reply(TransformationMetadata(Map(brandDependency -> ("test", 1L))))
      schemaManagerRouter.expectMsg(AddPartitions(List(view)))
      schemaManagerRouter.reply(TransformationMetadata(Map(view -> ("test", 1L))))
      schemaManagerRouter.expectMsg(AddPartitions(List(productDependency)))
      schemaManagerRouter.reply(TransformationMetadata(Map(productDependency -> ("test", 1L))))

      Await.result(future, TIMEOUT)
      future.isCompleted shouldBe true
      future.value.get.isSuccess shouldBe true
      future.value.get.get.asInstanceOf[ActorRef]
    }
  }

  trait SchedoscopeServiceWithViewManagerTest extends ViewManagerActorTest {

    Schedoscope.actorSystemBuilder = () => system

    lazy val service = new SchedoscopeServiceImpl(system,
      Schedoscope.settings,
      viewManagerActor,
      transformationManagerActor.ref)

  }

  trait SchedoscopeServiceTest {

    lazy val driver = mock[HiveDriver]
    lazy val hiveTransformation = mock[HiveTransformation]

    val viewManagerActor = TestProbe()
    val schemaManagerActor = TestProbe()
    val transformationManagerActor = TestProbe()

    // view actors (spawn by ViewManagerActor
    lazy val prodBrandViewActor = TestProbe()
    lazy val prodViewActor = TestProbe()
    lazy val brandViewActor = TestProbe()

    Schedoscope.actorSystemBuilder = () => system

    Schedoscope.viewManagerActorBuilder = () => viewManagerActor.ref
    Schedoscope.transformationManagerActorBuilder = () => transformationManagerActor.ref
    Schedoscope.schemaManagerRouterBuilder = () => schemaManagerActor.ref

    lazy val service = new SchedoscopeServiceImpl(system,
      Schedoscope.settings,
      viewManagerActor.ref,
      transformationManagerActor.ref)
  }

  trait SchedoscopeServiceExternalTest {
    lazy val settings = TestUtils.createSettings("schedoscope.external.enabled=true",
      """schedoscope.external.internal=["prod.app.test"] """ )
    Schedoscope.actorSystemBuilder = () => system

    val viewManagerActor = TestProbe()
    val transformationManagerActor = TestProbe()
    Schedoscope.viewManagerActorBuilder = () => viewManagerActor.ref

    lazy val service = new SchedoscopeServiceImpl(system,
      settings,
      viewManagerActor.ref,
      transformationManagerActor.ref)

  }


  "The ViewManagerActor" should "create a new view" in new SchedoscopeServiceWithViewManagerTest {
    initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)
  }

  /**
    * Testing /views
    *
    * /views/ViewPattern parameters:
    *
    * status=(transforming|nodata|materialized|failed|retrying|waiting) - passing this parameter will further restrict the output to views with the given state.
    * filter=Regexp - apply a regular expression filter on the view path to further limit information to certain views (e.g. '?filter=.Visit.')
    * dependencies=(true|false) - if a specific view is requested, setting this to true will also return information about all dependent views
    * overview=(true|false) - only return aggregate counts about view scheduling states and not information about individual views.
    *
    */

  it should "fail to load views due to completely invalid viewPathUrl passed" in new SchedoscopeServiceWithViewManagerTest {

    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)
    val wrongUrlPath = "plainOldWrong"
    val initError = s"Invalid view URL pattern passed: ${wrongUrlPath}.\n" +
      s"original Message: Error while parsing view(s) ${wrongUrlPath} : ${wrongUrlPath}\n" +
      "\nProblem: View URL paths needs at least a package and a view class name.\n"
    the [IllegalArgumentException] thrownBy {
      val response = service.views(Some(wrongUrlPath), Some(""), Some(""), Some(true), Some(true), Some(true))
      Await.result(response, TIMEOUT)
    } should have message initError + errorMsg
  }

  it should "fail to load views due to invalid View class passed" in new SchedoscopeServiceWithViewManagerTest {
    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)
    val packge = "test.views"
    val wrongClazz = "plainOldWrong"
    val wrongUrlPath = s"${packge}/${wrongClazz}/"
    val initError = s"Invalid view URL pattern passed: ${wrongUrlPath}.\n" +
      s"original Message: Error while parsing view(s) ${wrongUrlPath} : ${wrongUrlPath}\n" +
      s"\nProblem: No class for package and view: ${packge}.${wrongClazz}\n"
    the [IllegalArgumentException] thrownBy {
      val response = service.views(Some(wrongUrlPath), Some(""), Some(""), Some(true), Some(true), Some(true))
      Await.result(response, TIMEOUT)
    }  should have message initError + errorMsg
  }

  it should "fail to load views due to wrong package reference for View Brand " +
    "(in case you're wondering, means enumeration is working well)" in new SchedoscopeServiceWithViewManagerTest {

    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)
    val packge = "test.views"
    val wrongClazz = "plainOldWrong"
    val wrongUrlPath = s"${packge}/e(ProductBrand,Brand,Product,${wrongClazz})"
    val initError = s"Invalid view URL pattern passed: ${wrongUrlPath}.\n" +
      s"original Message: Error while parsing view(s) ${wrongUrlPath} : ${wrongUrlPath}\n" +
      s"\nProblem: No class for package and view: ${packge}.${wrongClazz}\n"
    the [IllegalArgumentException] thrownBy {
      val response = service.views(Some(wrongUrlPath), Some(""), Some(""), Some(true), Some(true), Some(true))
      Await.result(response, TIMEOUT)
    }  should have message initError + errorMsg
  }

  it should "Return only an overview of the current views" in new SchedoscopeServiceWithViewManagerTest {

    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)

    val response = service.views(Some(prodBrandUrl01), None, None, Some(true), Some(true), Some(true))
    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map(("receive", 3))
    response.value.get.get.views shouldBe List()
  }

  it should "initialize & get details about ONLY for that View when dependencies=false" in new SchedoscopeServiceWithViewManagerTest {

    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)
    val prodBrandviewUrlPath01 = Some(prodBrandUrl01)
    val statusParam = None
    val filterParam = None
    val dependenciesParam = Some(false)
    val overviewParam = Some(false)
    val allParam = Some(false)

    val response = service.views(prodBrandviewUrlPath01, statusParam, filterParam, dependenciesParam, overviewParam, allParam)
    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map(("receive", 1))
    response.value.get.get.views.size shouldBe 1
    response.value.get.get.views(0).status shouldBe initStatus

    response.value.get.get.views(0).viewPath shouldBe prodBrandUrl01 + s"/${year}${month}${day}"
    response.value.get.get.views(0).dependencies shouldBe None
  }

  it should "initialize & get details about a View and its dependencies provided that view" in new SchedoscopeServiceWithViewManagerTest {

    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)
    val prodBrandviewUrlPath01 = Some(prodBrandUrl01)
    val statusParam = None
    val filterParam = None
    val dependenciesParam = Some(true)
    val overviewParam = Some(false)
    val allParam = Some(false)

    val response = service.views(prodBrandviewUrlPath01, statusParam, filterParam, dependenciesParam, overviewParam, allParam)
    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map(("receive", 3))
    response.value.get.get.views.size shouldBe 3

    response.value.get.get.views(0).status shouldBe initStatus
    response.value.get.get.views(1).status shouldBe initStatus
    response.value.get.get.views(2).status shouldBe initStatus

    val resultViews = response.value.get.get.views
    val resultViewsList = List(resultViews(0), resultViews(1), resultViews(2)).sortBy(_.viewPath)
    resultViewsList(0).viewPath shouldBe brandUrl01
    resultViewsList(1).viewPath shouldBe prodUrl01 + s"/${year}${month}${day}"
    resultViewsList(2).viewPath shouldBe prodBrandUrl01 + s"/${year}${month}${day}"

    resultViewsList(0).dependencies shouldBe None
    resultViewsList(1).dependencies shouldBe None
    resultViewsList(2).dependencies.get.get(s"dev_test_views.brand_${shop01.toLowerCase}") shouldBe
      Some(List(brandUrl01))
    resultViewsList(2).dependencies.get.get(s"dev_test_views.product_${shop01.toLowerCase}") shouldBe
      Some(List(prodUrl01 + s"/${year}${month}${day}"))
  }

  it should "initialize & get details all View and their dependencies no view provided" in new SchedoscopeServiceWithViewManagerTest {

    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)
    val prodBrandviewUrlPath01 = None
    val statusParam = None
    val filterParam = None
    val dependenciesParam = Some(true)
    val overviewParam = Some(false)
    val allParam = Some(false)

    val response = service.views(prodBrandviewUrlPath01, statusParam, filterParam, dependenciesParam, overviewParam, allParam)
    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map(("receive", 3))
    response.value.get.get.views.size shouldBe 3
    response.value.get.get.views(0).status shouldBe initStatus
    response.value.get.get.views(1).status shouldBe initStatus
    response.value.get.get.views(2).status shouldBe initStatus

    val resultViews = response.value.get.get.views
    val resultViewsList = List(resultViews(0), resultViews(1), resultViews(2)).sortBy(_.viewPath)
    resultViewsList(0).viewPath shouldBe brandUrl01
    resultViewsList(1).viewPath shouldBe prodUrl01 + s"/${year}${month}${day}"
    resultViewsList(2).viewPath shouldBe prodBrandUrl01 + s"/${year}${month}${day}"

    resultViewsList(0).dependencies shouldBe None
    resultViewsList(1).dependencies shouldBe None
    resultViewsList(2).dependencies.get.get(s"dev_test_views.brand_${shop01.toLowerCase}") shouldBe
      Some(List(s"test.views/Brand/${shop01}"))
    resultViewsList(2).dependencies.get.get(s"dev_test_views.product_${shop01.toLowerCase}") shouldBe
      Some(List(prodUrl01 + s"/${year}${month}${day}"))
  }

  it should "initialize & get details of all Views and their " +
    "dependencies - view provided, and all=true" in new SchedoscopeServiceWithViewManagerTest {

    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)
    val prodBrandviewUrlPath01 = Some(prodBrandUrl01)
    val statusParam = None
    val filterParam = None
    val dependenciesParam = Some(true)
    val overviewParam = Some(false)
    val allParam = Some(true)

    val response = service.views(prodBrandviewUrlPath01, statusParam, filterParam, dependenciesParam, overviewParam, allParam)
    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map(("receive", 3))
    response.value.get.get.views.size shouldBe 6
  }

  it should "block a call on an external view" in new SchedoscopeServiceExternalTest {
    val testView = Brand(p("test"))

    the [IllegalArgumentException] thrownBy {
      val response = service.views(Some(testView.urlPath), None, None, None, None, None)
      Await.result(response, TIMEOUT)
    } should have message "Invalid view URL pattern passed: test.views/Brand/test.\n" +
      "original Message: You can not access an external view directly"
  }

  /**
    * Testing /materialize/ViewPattern
    *
    * /materialize/ViewPattern parameters:
    *
    * status=(transforming|nodata|materialized|failed|retrying|waiting) materialize all views that have a given status (e.g. 'failed')
    * mode=RESET_TRANSFORMATION_CHECKSUMS ignore transformation version checksums when detecting whether views need to be rematerialized. The new checksum overwrites the old checksum. Useful when changing the code of transformations in way that does not require recomputation.
    *   mode=RESET_TRANSFORMATION_CHECKSUMS_AND_TIMESTAMPS perform a "dry run" where transformation checksums and timestamps are set along the usual rules, however with no actual transformations taking place. As a result, all checksums in the metastore should be current and transformation timestamps should be consistent, such that no materialization will take place upon subsequent normal materializations.
    *   mode=TRANSFORM_ONLY materialize the given views, but without asking the views' dependencies to materialize as well. This is useful when a transformation higher up in the dependency lattice has failed and you want to retry it without potentially rematerializing all dependencies.
    *   mode=SET_ONLY force the given views into the materialized state. No transformation is performed, and all the views' transformation timestamps and checksums are set to current.
    *
    */

  it should "ask ViewManagerActor for View without dependencies, and send msg to correspondent Actor " +
    "to materialize the intended View with mocked viewActorManager" in new SchedoscopeServiceTest {
    val prodBrandviewUrlPath01 = Some(prodBrandUrl01)
    val statusParam = None
    val filterParam = None
    val modeParam = None
    val response = service.materialize(prodBrandviewUrlPath01, statusParam, filterParam, modeParam)

    viewManagerActor.expectMsg(GetViews(Some(List(productBrandView01)), statusParam, filterParam))

    viewManagerActor.reply(
      ViewStatusListResponse(List(ViewStatusResponse(initStatus, productBrandView01, prodBrandViewActor.ref))))

    prodBrandViewActor.expectMsg(MaterializeView(MaterializeViewMode.DEFAULT))

    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map(("receive", 1))
    response.value.get.get.views.size shouldBe 1
    response.value.get.get.views(0).status shouldBe initStatus
    response.value.get.get.views(0).viewPath shouldBe prodBrandUrl01 + s"/${year}${month}${day}"
    response.value.get.get.views(0).dependencies shouldBe None

  }

  it should "ask ViewManagerActor for View without dependencies, and send msg to correspondent Actor " +
    "to materialize the intended View with viewActorManager" in new SchedoscopeServiceWithViewManagerTest {
    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)

    val prodBrandviewUrlPath01 = Some(prodBrandUrl01)
    val statusParam = None
    val filterParam = None
    val modeParam = None
    val response = service.materialize(prodBrandviewUrlPath01, statusParam, filterParam, modeParam)

    Await.result(response, TIMEOUT)
    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map(("receive", 1))
    response.value.get.get.views.size shouldBe 1
    response.value.get.get.views(0).status shouldBe initStatus
    response.value.get.get.views(0).viewPath shouldBe prodBrandUrl01 + s"/${year}${month}${day}"
    response.value.get.get.views(0).dependencies shouldBe None
  }

  /**
    * Testing /newdata/ViewPattern
    *
    * /transformations parameters:
    *
    * status=(running|idle) passing this parameter will further restrict the output to transformation drivers with the given state.
    * filter=Regexp apply a regular expression filter on driver name (e.g. '?filter=.hive.')
    *
    */

  it should "ask ViewManagerActor for View without dependencies, and send msg 'newdata' to correspondent Actor " in
    new SchedoscopeServiceTest {
    val prodBrandviewUrlPath01 = Some(prodBrandUrl01)
    val statusParam = None
    val filterParam = None
    val response = service.newdata(prodBrandviewUrlPath01, statusParam, filterParam)

    viewManagerActor.expectMsg(GetViews(Some(List(productBrandView01)), statusParam, filterParam))

    viewManagerActor.reply(
      ViewStatusListResponse(List(ViewStatusResponse(initStatus, productBrandView01, prodBrandViewActor.ref))))

    prodBrandViewActor.expectMsg("newdata")

    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map(("receive", 1))
    response.value.get.get.views.size shouldBe 1
    response.value.get.get.views(0).status shouldBe initStatus
    response.value.get.get.views(0).viewPath shouldBe prodBrandUrl01 + s"/${year}${month}${day}"
    response.value.get.get.views(0).dependencies shouldBe None

  }

  /**
    * Testing /invalidate/ViewPattern
    *
    * /transformations parameters:
    *
    * status=(running|idle) passing this parameter will further restrict the output to transformation drivers with the given state.
    * filter=Regexp apply a regular expression filter on driver name (e.g. '?filter=.hive.')
    *
    */

  it should "ask ViewManagerActor for View without dependencies, and send msg to correspondent Actor " +
    "to invalidate the intended View with mocked viewActorManager" in new SchedoscopeServiceTest {
    val prodBrandviewUrlPath01 = Some(prodBrandUrl01)
    val statusParam = None
    val filterParam = None
    val modeParam = None
    val response = service.invalidate(prodBrandviewUrlPath01, statusParam, filterParam, modeParam)

    viewManagerActor.expectMsg(GetViews(Some(List(productBrandView01)), statusParam, filterParam))

    viewManagerActor.reply(
      ViewStatusListResponse(List(ViewStatusResponse(initStatus, productBrandView01, prodBrandViewActor.ref))))

    prodBrandViewActor.expectMsg(InvalidateView())

    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map(("receive", 1))
    response.value.get.get.views.size shouldBe 1
    response.value.get.get.views(0).status shouldBe initStatus
    response.value.get.get.views(0).viewPath shouldBe prodBrandUrl01 + s"/${year}${month}${day}"
    response.value.get.get.views(0).dependencies shouldBe None

  }

  it should "ask ViewManagerActor for View without dependencies, and send msg to correspondent Actor " +
    "to invalidate the intended View with viewActorManager" in new SchedoscopeServiceWithViewManagerTest {
    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)

    val prodBrandviewUrlPath01 = Some(prodBrandUrl01)
    val statusParam = None
    val filterParam = None
    val modeParam = None
    val response = service.invalidate(prodBrandviewUrlPath01, statusParam, filterParam, modeParam)

    Await.result(response, TIMEOUT)
    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map(("receive", 1))
    response.value.get.get.views.size shouldBe 1
    response.value.get.get.views(0).status shouldBe initStatus
    response.value.get.get.views(0).viewPath shouldBe prodBrandUrl01 + s"/${year}${month}${day}"
    response.value.get.get.views(0).dependencies shouldBe None
  }

  /**
    * Testing /queues
    *
    * /queues parameters:
    *
    * status=(running|idle) passing this parameter will further restrict the output to transformation drivers with the given state.
    * filter=Regexp apply a regular expression filter on driver name (e.g. '?filter=.hive.')
    *
    */

  it should "ask transformationManagerActor for ongoing queues" in new SchedoscopeServiceTest {
    val prodBrandviewUrlPath01 = Some(prodBrandUrl01)
    val typParam = None
    val filterParam = None
    val response = service.queues(typParam, filterParam)

    transformationManagerActor.expectMsg(DeployCommand())
    transformationManagerActor.expectMsg(GetQueues())

    val queueMsgStatus = QueueStatusListResponse(Map("allFakeActors" -> List(TestProbe().ref, TestProbe().ref)))
    transformationManagerActor.reply(queueMsgStatus)

    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map(("allFakeActors", 2))
    response.value.get.get.queues.get("allFakeActors").get.size shouldBe 2
  }

  /**
    * Testing /transformations
    *
    * /transformations parameters:
    *
    * status=(running|idle) passing this parameter will further restrict the output to transformation drivers with the given state.
    * filter=Regexp apply a regular expression filter on driver name (e.g. '?filter=.hive.')
    *
    */

  it should "ask transformationManagerActor for ongoing transformations " in new SchedoscopeServiceTest {
    val prodBrandviewUrlPath01 = Some(prodBrandUrl01)
    val statusParam = None
    val filterParam = None
    val modeParam = None
    val response = service.transformations(statusParam, filterParam)

    transformationManagerActor.expectMsg(DeployCommand())
    transformationManagerActor.expectMsg(GetTransformations())

    val transfMsgStatus = TransformationStatusResponse(message="transforming",
      transformationManagerActor.ref, driver=driver, driverRunHandle=driver.run(hiveTransformation)
      , driverRunStatus=DriverRunOngoing(driver, driver.run(hiveTransformation)))
    transformationManagerActor.reply(
      TransformationStatusListResponse(List(transfMsgStatus)))

    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map(("transforming", 1))
    response.value.get.get.transformations.size shouldBe 1
  }
}