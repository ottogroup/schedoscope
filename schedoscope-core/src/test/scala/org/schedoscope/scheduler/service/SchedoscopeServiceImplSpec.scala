package org.schedoscope.scheduler.service

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import akka.util.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.schedoscope.dsl.Parameter._
import org.schedoscope.dsl.View
import org.schedoscope.dsl.transformations.HiveTransformation
import org.schedoscope.scheduler.actors.ViewManagerActor
import org.schedoscope.scheduler.driver.{DriverRunOngoing, HiveDriver}
import org.schedoscope.scheduler.messages.{GetViews, ViewStatusListResponse, ViewStatusResponse, _}
import org.schedoscope.{Schedoscope, Settings, TestUtils}
import test.extviews.ExternalShop
import test.views.{Brand, ProductBrand}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Success

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
  val shop02 = "EC02"



  val prodBrandUrl01 = s"test.views/ProductBrand/${shop01}/${year}/${month}/${day}"
  val prodUrl01 = s"test.views/Product/${shop01}/${year}/${month}/${day}"
  val brandUrl01 = s"test.views/Brand/${shop01}"

  val productBrandView01 = ProductBrand(p(shop01), p(year), p(month), p(day))
  val brandDependency01: View = productBrandView01.dependencies.head
  val productDependency01: View = productBrandView01.dependencies(1)

  val productBrandView02 = ProductBrand(p(shop02), p(year), p(month), p(day))
  val brandDependency02: View = productBrandView02.dependencies.head
  val productDependency02: View = productBrandView02.dependencies(1)

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
    val viewSchedulingListenerManagerActor = TestProbe()
    Schedoscope.actorSystemBuilder = () => system

    val viewManagerActor = TestActorRef(
      ViewManagerActor.props(
        Schedoscope.settings,
        transformationManagerActor.ref,
        schemaManagerRouter.ref,
        viewSchedulingListenerManagerActor.ref))

    Schedoscope.viewManagerActorBuilder = () => viewManagerActor

    def initializeView(view: View): ActorRef = {
      val future = viewManagerActor ? view
      schemaManagerRouter.expectMsg(CheckOrCreateTables(List(view)))
      schemaManagerRouter.reply(SchemaActionSuccess())
      schemaManagerRouter.expectMsg(AddPartitions(List(view)))
      schemaManagerRouter.reply(TransformationMetadata(Map(view ->("test", 1L))))

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

    def initializeViewWithDep(view: View, brandDependency: View, productDependency: View): ActorRef = {
      val future = viewManagerActor ? view

      var messageSum = 0

      def acceptMessage: PartialFunction[Any, _] = {
        case AddPartitions(List(`brandDependency`)) =>
          schemaManagerRouter.reply(TransformationMetadata(Map(brandDependency ->("test", 1L))))
          messageSum += 1
        case AddPartitions(List(`productDependency`)) =>
          schemaManagerRouter.reply(TransformationMetadata(Map(productDependency ->("test", 1L))))
          messageSum += 2
        case AddPartitions(List(`view`)) =>
          schemaManagerRouter.reply(TransformationMetadata(Map(view ->("test", 1L))))
          messageSum += 3
        case CheckOrCreateTables(List(`brandDependency`)) =>
          schemaManagerRouter.reply(SchemaActionSuccess())
          messageSum += 4
        case CheckOrCreateTables(List(`productDependency`)) =>
          schemaManagerRouter.reply(SchemaActionSuccess())
          messageSum += 5
        case CheckOrCreateTables(List(`view`)) =>
          schemaManagerRouter.reply(SchemaActionSuccess())
          messageSum += 6
      }

      val msgs = schemaManagerRouter.receiveWhile(messages = 6)(acceptMessage)
      //msgs.size shouldBe 6
      //messageSum shouldBe 21

      //      Thread.sleep(2)

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

    lazy val settings = Settings()

    lazy val driver = mock[HiveDriver]
    lazy val hiveTransformation = mock[HiveTransformation]

    val viewManagerActor = TestProbe()
    val schemaManagerActor = TestProbe()
    val transformationManagerActor = TestProbe()

    // view actors (spawn by ViewManagerActor)
    lazy val prodBrandViewActor = TestProbe()
    lazy val prodViewActor = TestProbe()
    lazy val brandViewActor = TestProbe()

    Schedoscope.actorSystemBuilder = () => system

    Schedoscope.viewManagerActorBuilder = () => viewManagerActor.ref
    Schedoscope.transformationManagerActorBuilder = () => transformationManagerActor.ref
    Schedoscope.schemaManagerRouterBuilder = () => schemaManagerActor.ref

    lazy val service = new SchedoscopeServiceImpl(system,
      settings,
      viewManagerActor.ref,
      transformationManagerActor.ref)
  }

  trait SchedoscopeServiceExternalTest extends SchedoscopeServiceTest {
    override lazy val settings = TestUtils.createSettings("schedoscope.external-dependencies.enabled=true",
      """schedoscope.external-dependencies.home=["${env}.test.views"] """)
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
    the[IllegalArgumentException] thrownBy {
      val response = service.views(Some(wrongUrlPath), Some(""), Some(""), Some(""), Some(true), Some(true), Some(true))
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
    the[IllegalArgumentException] thrownBy {
      val response = service.views(Some(wrongUrlPath), Some(""), Some(""), Some(""), Some(true), Some(true), Some(true))
      Await.result(response, TIMEOUT)
    } should have message initError + errorMsg
  }

  it should "fail to initialize errorneous views" in new SchedoscopeServiceWithViewManagerTest {
    the[IllegalArgumentException] thrownBy {
      val response = service.materialize(Some("test.views/ViewWithExceptionThrowingDependency"), None, None, None, None)
      Await.result(response, TIMEOUT)
    } should have message "Invalid view passed to view manager for initialization"
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
    the[IllegalArgumentException] thrownBy {
      val response = service.views(Some(wrongUrlPath), Some(""), Some(""), Some(""), Some(true), Some(true), Some(true))
      Await.result(response, TIMEOUT)
    } should have message initError + errorMsg
  }

  it should "Return only an overview of the current views" in new SchedoscopeServiceWithViewManagerTest {

    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)

    val response = service.views(Some(prodBrandUrl01), None, None, None, Some(true), Some(true), Some(true))
    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map("receive" -> 3)
    response.value.get.get.views shouldBe List()
  }

  it should "initialize & get details about ONLY for that View when dependencies=false" in new SchedoscopeServiceWithViewManagerTest {

    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)
    val prodBrandviewUrlPath01 = Some(prodBrandUrl01)
    val statusParam = None
    val filterParam = None
    val issueFilterParam = None
    val dependenciesParam = Some(false)
    val overviewParam = Some(false)
    val allParam = Some(false)

    val response = service.views(prodBrandviewUrlPath01, statusParam, filterParam, issueFilterParam, dependenciesParam, overviewParam, allParam)
    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map(initStatus -> 1)
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
    val issueFilterParam = None
    val dependenciesParam = Some(true)
    val overviewParam = Some(false)
    val allParam = Some(false)

    val response = service.views(prodBrandviewUrlPath01, statusParam, filterParam, issueFilterParam, dependenciesParam, overviewParam, allParam)
    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map(initStatus -> 3)
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
    val issueFilterParam = None
    val dependenciesParam = Some(true)
    val overviewParam = Some(false)
    val allParam = Some(false)

    val response = service.views(prodBrandviewUrlPath01, statusParam, filterParam, issueFilterParam, dependenciesParam, overviewParam, allParam)
    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map(initStatus -> 3)
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

  it should "initialize & get details for View and its dependencies with dependencies=false + all=true" in new SchedoscopeServiceWithViewManagerTest {

    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)
    val prodBrandviewUrlPath01 = None
    val statusParam = None
    val filterParam = None
    val issueFilterParam = None
    val dependenciesParam = Some(false)
    val overviewParam = Some(false)
    val allParam = Some(true)

    val response = service.views(prodBrandviewUrlPath01, statusParam, filterParam, issueFilterParam, dependenciesParam, overviewParam, allParam)
    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map((initStatus, 3))
    response.value.get.get.views.size shouldBe 6
    response.value.get.get.views(0).status shouldBe initStatus
    response.value.get.get.views(1).status shouldBe initStatus
    response.value.get.get.views(2).status shouldBe initStatus

    val resultViews = response.value.get.get.views
    val resultViewsList = List(resultViews(3), resultViews(4), resultViews(5)).sortBy(_.viewPath)
    resultViewsList(0).viewPath shouldBe brandUrl01
    resultViewsList(1).viewPath shouldBe prodUrl01 + s"/${year}${month}${day}"
    resultViewsList(2).viewPath shouldBe prodBrandUrl01 + s"/${year}${month}${day}"

    resultViewsList(0).dependencies shouldBe None
    resultViewsList(1).dependencies shouldBe None
    resultViewsList(2).dependencies.get.get(s"dev_test_views.brand_${shop01.toLowerCase}") shouldBe
      Some(List(s"test.views/Brand/${shop01}"))
    resultViewsList(2).dependencies.get.get(s"dev_test_views.product_${shop01.toLowerCase}") shouldBe
      Some(List(prodUrl01 + s"/${year}${month}${day}"))

    val resultViewsList2 = List(resultViews(0), resultViews(1), resultViews(2))
    resultViewsList2(0).properties shouldBe None
    resultViewsList2(1).properties shouldBe None
    resultViewsList2(2).properties shouldBe None
  }

  it should "initialize & get details for View and its dependencies with dependencies=true + all=true" in new SchedoscopeServiceWithViewManagerTest {

    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)
    val prodBrandviewUrlPath01 = None
    val statusParam = None
    val filterParam = None
    val issueFilterParam = None
    val dependenciesParam = Some(true)
    val overviewParam = Some(false)
    val allParam = Some(true)

    val response = service.views(prodBrandviewUrlPath01, statusParam, filterParam, issueFilterParam, dependenciesParam, overviewParam, allParam)
    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map((initStatus, 3))
    response.value.get.get.views.size shouldBe 6
    response.value.get.get.views(0).status shouldBe initStatus
    response.value.get.get.views(1).status shouldBe initStatus
    response.value.get.get.views(2).status shouldBe initStatus

    val resultViews = response.value.get.get.views
    val resultViewsList = List(resultViews(3), resultViews(4), resultViews(5)).sortBy(_.viewPath)
    resultViewsList(0).viewPath shouldBe brandUrl01
    resultViewsList(1).viewPath shouldBe prodUrl01 + s"/${year}${month}${day}"
    resultViewsList(2).viewPath shouldBe prodBrandUrl01 + s"/${year}${month}${day}"

    resultViewsList(0).dependencies shouldBe None
    resultViewsList(1).dependencies shouldBe None
    resultViewsList(2).dependencies.get.get(s"dev_test_views.brand_${shop01.toLowerCase}") shouldBe
      Some(List(s"test.views/Brand/${shop01}"))
    resultViewsList(2).dependencies.get.get(s"dev_test_views.product_${shop01.toLowerCase}") shouldBe
      Some(List(prodUrl01 + s"/${year}${month}${day}"))

    val resultViewsList2 = List(resultViews(0), resultViews(1), resultViews(2))
    resultViewsList2(0).properties shouldBe None
    resultViewsList2(1).properties shouldBe None
    resultViewsList2(2).properties shouldBe None
  }

  it should "initialize & get details of all Views and their " +
    "dependencies - view provided, and all=true" in new SchedoscopeServiceWithViewManagerTest {

    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)
    val prodBrandviewUrlPath01 = Some(prodBrandUrl01)
    val statusParam = None
    val filterParam = None
    val issueFilterParam = None
    val dependenciesParam = Some(true)
    val overviewParam = Some(false)
    val allParam = Some(true)

    val response = service.views(prodBrandviewUrlPath01, statusParam, filterParam, issueFilterParam, dependenciesParam, overviewParam, allParam)
    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map("receive" -> 3)
    response.value.get.get.views.size shouldBe 6
  }

  it should "ignore irrelevant views of the same table" in new SchedoscopeServiceWithViewManagerTest {
    val prodBrandViewActor01 = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)
    val prodBrandViewActor02 = initializeViewWithDep(productBrandView02, brandDependency02, productDependency02)

    val response01 = service.views(Some(productBrandView01.urlPath), None, None, None, None, None, None)
    Await.result(response01, TIMEOUT)

    response01.value.get.get.views.size shouldBe 1

    val response02 = service.views(Some(productBrandView02.urlPath), None, None, None, None, None, None)
    Await.result(response02, TIMEOUT)

    response02.value.get.get.views.size shouldBe 1
  }

  it should "block a call on an external view" in new SchedoscopeServiceExternalTest {
    val testView = ExternalShop()

    val response = service.views(Some(testView.urlPath), None, None, None, None, None, None)

    the[IllegalArgumentException] thrownBy {
      Await.result(response, TIMEOUT)
    } should have message "Invalid view URL pattern passed: test.extviews/ExternalShop/.\n" +
      "original Message: You can not address an external view directly."
  }

  it should "allow a call on an internal view" in new SchedoscopeServiceExternalTest {
    val testView = Brand(p("test"))
    val response = service.views(Some(testView.urlPath), None, None, None, None, None, None)

    viewManagerActor.expectMsg(GetViews(Some(List(testView)), None, None, None))
    viewManagerActor.reply(ViewStatusListResponse(Success(List(ViewStatusResponse("loading", testView, viewManagerActor.ref)))))

    val expected = ViewStatusList(Map("loading" -> 1),
      List(ViewStatus("test.views/Brand/test",
        None, "loading", None, None, None, None, None, None, None, None, None, None, None)))

    whenReady(response) { result =>
      result shouldBe expected
    }
  }

  it should "filter views by dependencies and status param" in new SchedoscopeServiceWithViewManagerTest {

    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)
    // note - actor would in reality be incorrect, using it just for testing purposes
    val brandVSRmsg = ViewStatusResponse(status = "materialized", view = brandDependency01,
      actor = prodBrandViewActor, errors = Some(false), incomplete = Some(false))
    val prodVSRmsg = ViewStatusResponse(status = "materialized", view = productDependency01,
      actor = prodBrandViewActor, errors = Some(false), incomplete = Some(false))

    val brandExpected = ViewStatus(viewPath = brandDependency01.urlPath,
      viewTableName = None, status = "materialized",
      properties = Some(Map("errors" -> "false", "incomplete" -> "false")),
      fields = None, parameters = None, dependencies = None, lineage = None,
      transformation = None, export = None, storageFormat = None, materializeOnce = None,
      comment = None, isTable = None)

    val prodExpected = ViewStatus(viewPath = productDependency01.urlPath,
      viewTableName = None, status = "materialized",
      properties = Some(Map("errors" -> "false", "incomplete" -> "false")),
      fields = None, parameters = None, dependencies = None, lineage = None,
      transformation = None, export = None, storageFormat = None, materializeOnce = None,
      comment = None, isTable = None)

    viewManagerActor ? brandVSRmsg
    viewManagerActor ? prodVSRmsg

    val response = service.views(Some(prodBrandUrl01), Some("materialized"), None, None,
      Some(true), Some(false), Some(false))
    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map("materialized" -> 2)
    response.value.get.get.views should contain(brandExpected)
    response.value.get.get.views should contain(prodExpected)
  }

  it should "filter views by dependencies and status = materialized and issues with error=true AND incomplete=true" in new SchedoscopeServiceWithViewManagerTest {

    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)
    // note - actor would in reality be incorrect, using it just for testing purposes
    val brandVSRmsg = ViewStatusResponse(status = "transforming", view = brandDependency01,
      actor = prodBrandViewActor, errors = Some(true), incomplete = Some(true))
    val prodVSRmsg = ViewStatusResponse(status = "materialized", view = productDependency01,
      actor = prodBrandViewActor, errors = Some(true), incomplete = Some(true))

    val brandExpected = ViewStatus(viewPath = brandDependency01.urlPath,
      viewTableName = None, status = "materialized",
      properties = Some(Map("errors" -> "true", "incomplete" -> "true")),
      fields = None, parameters = None, dependencies = None, lineage = None, transformation = None,
      export = None, storageFormat = None, materializeOnce = None, comment = None,
      isTable = None)

    val prodExpected = ViewStatus(viewPath = productDependency01.urlPath,
      viewTableName = None, status = "materialized",
      properties = Some(Map("errors" -> "true", "incomplete" -> "true")),
      fields = None, parameters = None, dependencies = None, lineage = None, transformation = None,
      export = None, storageFormat = None, materializeOnce = None, comment = None,
      isTable = None)

    viewManagerActor ? brandVSRmsg
    viewManagerActor ? prodVSRmsg

    val response = service.views(Some(prodBrandUrl01), Some("materialized"), None, Some("errorsANDincomplete"),
      Some(true), Some(false), Some(false))
    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map("materialized" -> 1)
    response.value.get.get.views.head shouldBe prodExpected
  }

  it should "filter views by dependencies and issues with error=true AND incomplete=true and " +
    "return List of 2 elements" in new SchedoscopeServiceWithViewManagerTest {

    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)
    // note - actor would in reality be incorrect, using it just for testing purposes
    val brandVSRmsg = ViewStatusResponse(status = "materialized", view = brandDependency01,
      actor = prodBrandViewActor, errors = Some(true), incomplete = Some(true))
    val prodVSRmsg = ViewStatusResponse(status = "materialized", view = productDependency01,
      actor = prodBrandViewActor, errors = Some(true), incomplete = Some(true))

    val brandExpected = ViewStatus(viewPath = brandDependency01.urlPath,
      viewTableName = None, status = "materialized",
      properties = Some(Map("errors" -> "true", "incomplete" -> "true")),
      fields = None, parameters = None, dependencies = None, lineage = None, transformation = None,
      export = None, storageFormat = None, materializeOnce = None, comment = None,
      isTable = None)

    val prodExpected = ViewStatus(viewPath = productDependency01.urlPath,
      viewTableName = None, status = "materialized",
      properties = Some(Map("errors" -> "true", "incomplete" -> "true")),
      fields = None, parameters = None, dependencies = None, lineage = None, transformation = None,
      export = None, storageFormat = None, materializeOnce = None, comment = None,
      isTable = None)

    viewManagerActor ? brandVSRmsg
    viewManagerActor ? prodVSRmsg

    val response = service.views(Some(prodBrandUrl01), None, None, Some("errorsANDincomplete"),
      Some(true), Some(false), Some(false))
    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map("materialized" -> 2)
    response.value.get.get.views should contain(brandExpected)
    response.value.get.get.views should contain(prodExpected)
  }

  it should "filter views by dependencies and issues with error=true AND incomplete=true " +
    "and return list of 1 element" in new SchedoscopeServiceWithViewManagerTest {

    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)
    // note - actor would in reality be incorrect, using it just for testing purposes
    val brandVSRmsg = ViewStatusResponse(status = "materialized", view = brandDependency01,
      actor = prodBrandViewActor, errors = Some(false), incomplete = Some(true))
    val prodVSRmsg = ViewStatusResponse(status = "materialized", view = productDependency01,
      actor = prodBrandViewActor, errors = Some(true), incomplete = Some(true))

    val brandExpected = ViewStatus(viewPath = brandDependency01.urlPath,
      viewTableName = None, status = "materialized",
      properties = Some(Map("errors" -> "true", "incomplete" -> "true")),
      fields = None, parameters = None, dependencies = None, lineage = None, transformation = None,
      export = None, storageFormat = None, materializeOnce = None, comment = None,
      isTable = None)

    val prodExpected = ViewStatus(viewPath = productDependency01.urlPath,
      viewTableName = None, status = "materialized",
      properties = Some(Map("errors" -> "true", "incomplete" -> "true")),
      fields = None, parameters = None, dependencies = None, lineage = None, transformation = None,
      export = None, storageFormat = None, materializeOnce = None, comment = None,
      isTable = None)

    viewManagerActor ? brandVSRmsg
    viewManagerActor ? prodVSRmsg

    val response = service.views(Some(prodBrandUrl01), None, None, Some("errorsANDincomplete"),
      Some(true), Some(false), Some(false))
    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map("materialized" -> 1)
    response.value.get.get.views.head shouldBe prodExpected
  }

  it should "filter views by dependencies and issues with error=false AND incomplete=true" in new SchedoscopeServiceWithViewManagerTest {

    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)
    // note - actor would in reality be incorrect, using it just for testing purposes
    val brandVSRmsg = ViewStatusResponse(status = "materialized", view = brandDependency01,
      actor = prodBrandViewActor, errors = Some(false), incomplete = Some(true))
    val prodVSRmsg = ViewStatusResponse(status = "materialized", view = productDependency01,
      actor = prodBrandViewActor, errors = Some(false), incomplete = Some(true))

    val brandExpected = ViewStatus(viewPath = brandDependency01.urlPath,
      viewTableName = None, status = "materialized",
      properties = Some(Map("errors" -> "false", "incomplete" -> "true")),
      fields = None, parameters = None, dependencies = None, lineage = None, transformation = None,
      export = None, storageFormat = None, materializeOnce = None, comment = None,
      isTable = None)

    val prodExpected = ViewStatus(viewPath = productDependency01.urlPath,
      viewTableName = None, status = "materialized",
      properties = Some(Map("errors" -> "false", "incomplete" -> "true")),
      fields = None, parameters = None, dependencies = None, lineage = None, transformation = None,
      export = None, storageFormat = None, materializeOnce = None, comment = None,
      isTable = None)

    viewManagerActor ? brandVSRmsg
    viewManagerActor ? prodVSRmsg

    val response = service.views(Some(prodBrandUrl01), None, None, Some("incomplete"),
      Some(true), Some(false), Some(false))
    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map("materialized" -> 2)
    response.value.get.get.views should contain(brandExpected)
    response.value.get.get.views should contain(prodExpected)
  }

  it should "filter views by dependencies and issues with error=true and return empty list" in new SchedoscopeServiceWithViewManagerTest {

    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)
    // note - actor would in reality be incorrect, using it just for testing purposes
    val brandVSRmsg = ViewStatusResponse(status = "materialized", view = brandDependency01,
      actor = prodBrandViewActor, errors = Some(false), incomplete = Some(true))
    val prodVSRmsg = ViewStatusResponse(status = "materialized", view = productDependency01,
      actor = prodBrandViewActor, errors = Some(false), incomplete = Some(true))

    viewManagerActor ? brandVSRmsg
    viewManagerActor ? prodVSRmsg

    val response = service.views(Some(prodBrandUrl01), None, None, Some("errors"),
      Some(true), Some(false), Some(false))
    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview.size shouldBe 0
    response.value.get.get.views.size shouldBe 0
  }

  /**
    * Testing /materialize/ViewPattern
    *
    * /materialize/ViewPattern parameters:
    *
    * status=(transforming|nodata|materialized|failed|retrying|waiting) materialize all views that have a given status (e.g. 'failed')
    * mode=RESET_TRANSFORMATION_CHECKSUMS ignore transformation version checksums when detecting whether views need to be rematerialized. The new checksum overwrites the old checksum. Useful when changing the code of transformations in way that does not require recomputation.
    * mode=RESET_TRANSFORMATION_CHECKSUMS_AND_TIMESTAMPS perform a "dry run" where transformation checksums and timestamps are set along the usual rules, however with no actual transformations taking place. As a result, all checksums in the metastore should be current and transformation timestamps should be consistent, such that no materialization will take place upon subsequent normal materializations.
    * mode=TRANSFORM_ONLY materialize the given views, but without asking the views' dependencies to materialize as well. This is useful when a transformation higher up in the dependency lattice has failed and you want to retry it without potentially rematerializing all dependencies.
    * mode=SET_ONLY force the given views into the materialized state. No transformation is performed, and all the views' transformation timestamps and checksums are set to current.
    *
    */

  it should "ask ViewManagerActor for View without dependencies, and send msg to correspondent Actor " +
    "to materialize the intended View with mocked viewActorManager" in new SchedoscopeServiceTest {
    val prodBrandviewUrlPath01 = Some(prodBrandUrl01)
    val statusParam = None
    val filterParam = None
    val issueFilterParam = None
    val modeParam = None
    val response = service.materialize(prodBrandviewUrlPath01, statusParam, filterParam, issueFilterParam, modeParam)

    viewManagerActor.expectMsg(GetViews(Some(List(productBrandView01)), statusParam, filterParam, issueFilterParam))

    // Note: response will never be the newState => in reality,
    // it will be always the original state; however, for testing purposes
    // changed the artificially created different ViewStatusResponse values
    viewManagerActor.reply(
      ViewStatusListResponse(Success(List(ViewStatusResponse("materialized", productBrandView01, prodBrandViewActor.ref,
        errors = Some(false), incomplete = Some(false))))))

    prodBrandViewActor.expectMsg(CommandForView(None, productBrandView01, MaterializeView(MaterializeViewMode.DEFAULT)))

    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map("materialized" -> 1)
    response.value.get.get.views.size shouldBe 1
    response.value.get.get.views(0).status shouldBe "materialized"
    response.value.get.get.views(0).viewPath shouldBe prodBrandUrl01 + s"/${year}${month}${day}"
    response.value.get.get.views(0).dependencies shouldBe None
    response.value.get.get.views(0).properties shouldBe Some(Map("errors" -> "false", "incomplete" -> "false"))

  }

  it should "ask ViewManagerActor for View without dependencies, and send msg to correspondent Actor " +
    "to materialize the intended View with viewActorManager" in new SchedoscopeServiceWithViewManagerTest {
    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)

    val prodBrandviewUrlPath01 = Some(prodBrandUrl01)
    val statusParam = None
    val filterParam = None
    val issueFilterParam = None
    val modeParam = None
    val response = service.materialize(prodBrandviewUrlPath01, statusParam, filterParam, issueFilterParam, modeParam)

    Await.result(response, TIMEOUT)
    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map(initStatus -> 1)
    response.value.get.get.views.size shouldBe 1
    response.value.get.get.views(0).status shouldBe initStatus
    response.value.get.get.views(0).viewPath shouldBe prodBrandUrl01 + s"/${year}${month}${day}"
    response.value.get.get.views(0).dependencies shouldBe None
    response.value.get.get.views(0).properties shouldBe None
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
      val issueFilterParam = None
      val response = service.newdata(prodBrandviewUrlPath01, statusParam, filterParam)

      viewManagerActor.expectMsg(GetViews(Some(List(productBrandView01)), statusParam, filterParam, issueFilterParam))

      viewManagerActor.reply(
        ViewStatusListResponse(Success(List(ViewStatusResponse(initStatus, productBrandView01, prodBrandViewActor.ref)))))

      prodBrandViewActor.expectMsg("newdata")

      Await.result(response, TIMEOUT)

      response.isCompleted shouldBe true
      response.value.get.get.overview shouldBe Map(initStatus -> 1)
      response.value.get.get.views.size shouldBe 1
      response.value.get.get.views(0).status shouldBe initStatus
      response.value.get.get.views(0).viewPath shouldBe prodBrandUrl01 + s"/${year}${month}${day}"
      response.value.get.get.views(0).dependencies shouldBe None
      response.value.get.get.views(0).properties shouldBe None

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
    val issueFilterParam = None
    val modeParam = None
    val response = service.invalidate(prodBrandviewUrlPath01, statusParam, filterParam, issueFilterParam, modeParam)

    viewManagerActor.expectMsg(GetViews(Some(List(productBrandView01)), statusParam, filterParam, issueFilterParam))

    // Note: response will never be the newState => in reality,
    // it will be always the original state; however, for testing purposes
    // changed the artificially created different ViewStatusResponse values
    viewManagerActor.reply(
      ViewStatusListResponse(Success(List(ViewStatusResponse("invalidated", productBrandView01, prodBrandViewActor.ref,
        errors = Some(true), incomplete = Some(true))))))

    prodBrandViewActor.expectMsg(CommandForView(None, productBrandView01, InvalidateView()))

    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map("invalidated" -> 1)
    response.value.get.get.views.size shouldBe 1
    response.value.get.get.views(0).status shouldBe "invalidated"
    response.value.get.get.views(0).viewPath shouldBe prodBrandUrl01 + s"/${year}${month}${day}"
    response.value.get.get.views(0).dependencies shouldBe None
    response.value.get.get.views(0).properties shouldBe None

  }

  it should "ask ViewManagerActor for View without dependencies, and send msg to correspondent Actor " +
    "to invalidate the intended View with viewActorManager" in new SchedoscopeServiceWithViewManagerTest {
    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)

    val prodBrandviewUrlPath01 = Some(prodBrandUrl01)
    val statusParam = None
    val filterParam = None
    val issueFilterParam = None
    val modeParam = None
    val response = service.invalidate(prodBrandviewUrlPath01, statusParam, filterParam, issueFilterParam, modeParam)

    Await.result(response, TIMEOUT)
    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map("receive" -> 1)
    response.value.get.get.views.size shouldBe 1
    response.value.get.get.views(0).status shouldBe initStatus
    response.value.get.get.views(0).viewPath shouldBe prodBrandUrl01 + s"/${year}${month}${day}"
    response.value.get.get.views(0).dependencies shouldBe None
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

    val transfMsgStatus = TransformationStatusResponse(message = "transforming",
      transformationManagerActor.ref, driver = driver, driverRunHandle = driver.run(hiveTransformation)
      , driverRunStatus = DriverRunOngoing(driver, driver.run(hiveTransformation)))
    transformationManagerActor.reply(
      TransformationStatusListResponse(List(transfMsgStatus)))

    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map("transforming" -> 1)
    response.value.get.get.transformations.size shouldBe 1
  }
}

