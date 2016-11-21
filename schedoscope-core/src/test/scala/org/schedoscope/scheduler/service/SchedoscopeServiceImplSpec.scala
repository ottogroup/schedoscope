package org.schedoscope.scheduler.service

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.schedoscope.scheduler.actors.ViewManagerActor
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.schedoscope.dsl.Parameter._
import org.schedoscope.dsl.View
import org.schedoscope.scheduler.messages._
import org.schedoscope.Schedoscope
import test.views.ProductBrand

import scala.concurrent.Await
import scala.concurrent.duration._



class SchedoscopeServiceImplSpec extends TestKit(ActorSystem("schedoscope"))
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll
  with ImplicitSender {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  // defaults
  lazy val errorMsg = "\nLittle path format guide\n========================\n\n/{package}/{view}(/{view parameter value})*\n\nView parameter value format:\n  i(aNumber)                    => an integer\n  l(aNumber)                    => a long\n  b(aNumber)                    => a byte\n  t(true)|t(false)              => a boolean\n  f(aFloat)                     => a float\n  d(aDouble)                    => a double\n  ym(yyyyMM)                    => a MonthlyParameterization\n  ymd(yyyyMMdd)                 => a DailyParameterization\n  null()                        => null\n  everything else               => a string\n\nRanges on view parameter values:\n  rym(yyyyMM-yyyyMM)            => all MonthlyParameterizations between the first (earlier) and the latter (later)\n  rymd(yyyyMMdd-yyyyMMdd)       => all DailyParameterizations between the first (earlier) and the latter (later)\n  e{constructor parameter value format}({aValue},{anotherValue})\n                                => enumerate multiple values for a given view parameter value format.\n  For instance: \n    ei(1,2,3)                   => an enumeration of integer view parameters \n    e(aString, anotherString)   => an enumeration of string view parameters \n    eymd(yyyyMM,yyyMM)          => an enumeration of MonthlyParameterizations\n    erymd(yyyyMM-yyyyMM,yyyyMM-yyyyMM) => an enumeration of MonthlyParameterization ranges\n\nQuoting:\n  Use backslashes to escape the syntax given above. The following characters need quotation: \\,(-)\n"

  // dummy data
  val year = "2014"
  val month = "01"
  val day = "01"
  val shop01 = "EC01"
  val shop02 = "EC02"

  val productBrandView01 = ProductBrand(p(shop01), p(year), p(month), p(day))
  val brandDependency01:View = productBrandView01.dependencies.head
  val productDependency01:View = productBrandView01.dependencies(1)

  val productBrandView02 = ProductBrand(p(shop02), p(year), p(month), p(day))
  val brandDependency02:View = productBrandView02.dependencies.head
  val productDependency02:View = productBrandView02.dependencies(1)

  // views Status along their lifecycle
  val initStatus = "receive"


  trait ViewManagerActorTest {

    implicit val timeout = Timeout(5 seconds)

    val schemaManagerActor = TestProbe()
    val actionsManagerActor = TestProbe()
    val transformationManagerActor = TestProbe()
    val config = ConfigFactory.load()

    Schedoscope.actorSystemBuilder = () => system

    val viewManagerActor = TestActorRef(
      ViewManagerActor.props(
            Schedoscope.settings,
            actionsManagerActor.ref,
            schemaManagerActor.ref,
            schemaManagerActor.ref)
    )

    Schedoscope.viewManagerActorBuilder = () => viewManagerActor

    def initializeView(view: View): ActorRef = {
      val future = viewManagerActor ? view
      schemaManagerActor.expectMsg(CheckOrCreateTables(List(view)))
      schemaManagerActor.reply(SchemaActionSuccess())
      schemaManagerActor.expectMsg(AddPartitions(List(view)))
      schemaManagerActor.reply(TransformationMetadata(Map(view -> ("test", 1L))))

      Await.result(future, 5 seconds)
      future.isCompleted shouldBe true
      future.value.get.isSuccess shouldBe true
      future.value.get.get.asInstanceOf[ActorRef]
    }

    def getInitializedView(view: View): ActorRef = {
      val future = viewManagerActor ? view
      Await.result(future, 5 seconds)
      future.isCompleted shouldBe true
      future.value.get.isSuccess shouldBe true
      future.value.get.get.asInstanceOf[ActorRef]
    }

    def initializeViewWithDep(view: View, brandDependency:View, productDependency:View): ActorRef = {
      val future = viewManagerActor ? view

      schemaManagerActor.expectMsg(CheckOrCreateTables(List(brandDependency)))
      schemaManagerActor.reply(SchemaActionSuccess())
      schemaManagerActor.expectMsg(CheckOrCreateTables(List(view)))
      schemaManagerActor.reply(SchemaActionSuccess())
      schemaManagerActor.expectMsg(CheckOrCreateTables(List(productDependency)))
      schemaManagerActor.reply(SchemaActionSuccess())
      schemaManagerActor.expectMsg(AddPartitions(List(brandDependency)))
      schemaManagerActor.reply(TransformationMetadata(Map(brandDependency -> ("test", 1L))))
      schemaManagerActor.expectMsg(AddPartitions(List(view)))
      schemaManagerActor.reply(TransformationMetadata(Map(view -> ("test", 1L))))
      schemaManagerActor.expectMsg(AddPartitions(List(productDependency)))
      schemaManagerActor.reply(TransformationMetadata(Map(productDependency -> ("test", 1L))))

      Await.result(future, 5 seconds)
      future.isCompleted shouldBe true
      future.value.get.isSuccess shouldBe true
      future.value.get.get.asInstanceOf[ActorRef]
    }

  }

  trait SchedoscopeServiceTest extends ViewManagerActorTest {

    Schedoscope.actorSystemBuilder = () => system

    lazy val service = new SchedoscopeServiceImpl(system,
      Schedoscope.settings,
      viewManagerActor,
      transformationManagerActor.ref)

  }


  "The ViewManagerActor" should "create a new view" in new SchedoscopeServiceTest {
    initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)
  }


  it should "fail to load views due to completely invalid viewPathUrl passed" in new SchedoscopeServiceTest {

    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)
    val wrongUrlPath = "plainOldWrong"
    val initError = s"Invalid view URL pattern passed: ${wrongUrlPath}.\n" +
      s"original Message: Error while parsing view(s) ${wrongUrlPath} : ${wrongUrlPath}\n" +
      "\nProblem: View URL paths needs at least a package and a view class name.\n"
    the [IllegalArgumentException] thrownBy {
      val response = service.views(Some(wrongUrlPath), Some(""), Some(""), Some(true), Some(true), Some(true))
      Await.result(response, 10 seconds)
    } should have message initError + errorMsg
  }

  it should "fail to load views due to invalid View class passed" in new SchedoscopeServiceTest {
    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)
    val packge = "test.views"
    val wrongClazz = "plainOldWrong"
    val wrongUrlPath = s"${packge}/${wrongClazz}/"
    val initError = s"Invalid view URL pattern passed: ${wrongUrlPath}.\n" +
      s"original Message: Error while parsing view(s) ${wrongUrlPath} : ${wrongUrlPath}\n" +
      s"\nProblem: No class for package and view: ${packge}.${wrongClazz}\n"
    the [IllegalArgumentException] thrownBy {
      val response = service.views(Some(wrongUrlPath), Some(""), Some(""), Some(true), Some(true), Some(true))
      Await.result(response, 10 seconds)
    }  should have message initError + errorMsg
  }

  it should "fail to load views due to wrong package reference for View Brand " +
    "(in case you're wondering, means enumeration is working well)" in new SchedoscopeServiceTest {

    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)
    val packge = "test.views"
    val wrongClazz = "plainOldWrong"
    val wrongUrlPath = s"${packge}/e(ProductBrand,Brand,Product,${wrongClazz})"
    val initError = s"Invalid view URL pattern passed: ${wrongUrlPath}.\n" +
      s"original Message: Error while parsing view(s) ${wrongUrlPath} : ${wrongUrlPath}\n" +
      s"\nProblem: No class for package and view: ${packge}.${wrongClazz}\n"
    the [IllegalArgumentException] thrownBy {
      val response = service.views(Some(wrongUrlPath), Some(""), Some(""), Some(true), Some(true), Some(true))
      Await.result(response, 10 seconds)
    }  should have message initError + errorMsg
  }

  it should "Return only an overview of the current views" in new SchedoscopeServiceTest {

    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)

    val prodBrandviewUrlPath01 = Some(s"test.views/ProductBrand/${shop01}/${year}/${month}/${day}")

    val response = service.views(prodBrandviewUrlPath01, None, None, Some(true), Some(true), Some(true))
    Await.result(response, 10 seconds)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map(("receive", 3))
    response.value.get.get.views shouldBe List()
  }

  it should "initialize & get details about ONLY for that View when dependencies=false" in new SchedoscopeServiceTest {

    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)
    val prodBrandviewUrlPath01 = Some(s"test.views/ProductBrand/${shop01}/${year}/${month}/${day}")
    val statusParam = None
    val filterParam = None
    val dependenciesParam = Some(false)
    val overviewParam = Some(false)
    val allParam = Some(false)

    val response = service.views(prodBrandviewUrlPath01, statusParam,
      filterParam, dependenciesParam, overviewParam, allParam)
    Await.result(response, 10 seconds)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map(("receive", 1))
    response.value.get.get.views.size shouldBe 1
    response.value.get.get.views(0).status shouldBe initStatus

    response.value.get.get.views(0).viewPath shouldBe
      s"test.views/ProductBrand/${shop01}/${year}/${month}/${day}/${year}${month}${day}"
    response.value.get.get.views(0).dependencies shouldBe None
  }

  it should "initialize & get details about a View and its dependencies provided that view" in new SchedoscopeServiceTest {

    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)
    val prodBrandviewUrlPath01 = Some(s"test.views/ProductBrand/${shop01}/${year}/${month}/${day}")
    val statusParam = None
    val filterParam = None
    val dependenciesParam = Some(true)
    val overviewParam = Some(false)
    val allParam = Some(false)

    val response = service.views(prodBrandviewUrlPath01, statusParam, filterParam, dependenciesParam, overviewParam, allParam)
    Await.result(response, 10 seconds)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map(("receive", 3))
    response.value.get.get.views.size shouldBe 3

    response.value.get.get.views(0).status shouldBe initStatus
    response.value.get.get.views(1).status shouldBe initStatus
    response.value.get.get.views(2).status shouldBe initStatus

    val resultViews = response.value.get.get.views
    val resultViewsList = List(resultViews(0), resultViews(1), resultViews(2)).sortBy(_.viewPath)
    resultViewsList(0).viewPath shouldBe s"test.views/Brand/${shop01}"
    resultViewsList(1).viewPath shouldBe s"test.views/Product/${shop01}/${year}/${month}/${day}/${year}${month}${day}"
    resultViewsList(2).viewPath shouldBe
      s"test.views/ProductBrand/${shop01}/${year}/${month}/${day}/${year}${month}${day}"

    resultViewsList(0).dependencies shouldBe None
    resultViewsList(1).dependencies shouldBe None
    resultViewsList(2).dependencies.get.get(s"dev_test_views.brand_${shop01.toLowerCase}") shouldBe
      Some(List(s"test.views/Brand/${shop01}"))
    resultViewsList(2).dependencies.get.get(s"dev_test_views.product_${shop01.toLowerCase}") shouldBe
      Some(List(s"test.views/Product/${shop01}/${year}/${month}/${day}/${year}${month}${day}"))

  }

  it should "initialize & get details all View and their dependencies no view provided" in new SchedoscopeServiceTest {

    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)
    val prodBrandviewUrlPath01 = None
    val statusParam = None
    val filterParam = None
    val dependenciesParam = Some(true)
    val overviewParam = Some(false)
    val allParam = Some(false)

    val response = service.views(prodBrandviewUrlPath01, statusParam, filterParam, dependenciesParam, overviewParam, allParam)
    Await.result(response, 10 seconds)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map(("receive", 3))
    response.value.get.get.views.foreach(println)
    response.value.get.get.views.size shouldBe 3
    response.value.get.get.views(0).status shouldBe initStatus
    response.value.get.get.views(1).status shouldBe initStatus
    response.value.get.get.views(2).status shouldBe initStatus

    val resultViews = response.value.get.get.views
    val resultViewsList = List(resultViews(0), resultViews(1), resultViews(2)).sortBy(_.viewPath)
    resultViewsList(0).viewPath shouldBe s"test.views/Brand/${shop01}"
    resultViewsList(1).viewPath shouldBe s"test.views/Product/${shop01}/${year}/${month}/${day}/${year}${month}${day}"
    resultViewsList(2).viewPath shouldBe
      s"test.views/ProductBrand/${shop01}/${year}/${month}/${day}/${year}${month}${day}"

    resultViewsList(0).dependencies shouldBe None
    resultViewsList(1).dependencies shouldBe None
    resultViewsList(2).dependencies.get.get(s"dev_test_views.brand_${shop01.toLowerCase}") shouldBe
      Some(List(s"test.views/Brand/${shop01}"))
    resultViewsList(2).dependencies.get.get(s"dev_test_views.product_${shop01.toLowerCase}") shouldBe
      Some(List(s"test.views/Product/${shop01}/${year}/${month}/${day}/${year}${month}${day}"))
  }

  it should "initialize & get details of all Views and their dependencies - view provided, and all=true" in new SchedoscopeServiceTest {

    val prodBrandViewActor = initializeViewWithDep(productBrandView01, brandDependency01, productDependency01)
    val prodBrandviewUrlPath01 = Some(s"test.views/ProductBrand/${shop01}/${year}/${month}/${day}")
    val statusParam = None
    val filterParam = None
    val dependenciesParam = Some(true)
    val overviewParam = Some(false)
    val allParam = Some(true)

    val response = service.views(prodBrandviewUrlPath01, statusParam, filterParam, dependenciesParam, overviewParam, allParam)
    Await.result(response, 10 seconds)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map(("receive", 3))
    response.value.get.get.views.foreach(println)
    response.value.get.get.views.size shouldBe 6
  }

  /*
  it should "materialze a View and its dependencies" in new SchedoscopeServiceTest {

  }
  */

}