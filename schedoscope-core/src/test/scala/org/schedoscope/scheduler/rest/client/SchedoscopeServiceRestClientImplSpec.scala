package org.schedoscope.scheduler.rest.client


import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.scalatest.{FlatSpecLike, Matchers}
import org.schedoscope.Schedoscope
import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._
import spray.http._
import org.schedoscope.scheduler.service.RunStatus


class SchedoscopeServiceRestClientImplSpec extends FlatSpecLike
  with Matchers
  with MockitoSugar {

  // dummy data
  val year = "2014"
  val month = "01"
  val day = "01"
  val shop01 = "EC01"

  val pakkage = "test.views"
  val prodBrandUrl01 = s"${pakkage}/ProductBrand/${shop01}/${year}/${month}/${day}"
  val prodUrl01 = s"${pakkage}/Product/${shop01}/${year}/${month}/${day}"
  val brandUrl01 = s"${pakkage}/Brand/${shop01}"
  val TIMEOUT = 5 seconds

  /* Testing Marshalling w Mocking */
  val mockResponse = mock[HttpResponse]
  val mockStatus = mock[StatusCode]
  when(mockResponse.status).thenReturn(mockStatus)
  when(mockStatus.isSuccess).thenReturn(true)

  trait schedoscopeClientTest {
    def initialize(json: String, host:String=Schedoscope.settings.host,
                   port:Int=Schedoscope.settings.port) = {
      val body = HttpEntity(ContentTypes.`application/json`, json.getBytes())
      when(mockResponse.entity).thenReturn(body)
      val dummyClient = new SchedoscopeServiceRestClientImpl(host, port) {
        override def sendAndReceive = {

          (req:HttpRequest) => Promise.successful(mockResponse).future
        }
      }
      dummyClient
    }
  }


  it should "correctly parse /views request and unmarshal json ProductBrand object with single view" in
    new schedoscopeClientTest {
    val json =
      s"""
           {
             "overview": {
               "receive": 1
             },
            "views": [
            {
              "viewPath": "${pakkage}/ProductBrand/${shop01}/${year}/${month}/${day}/${year}${month}${day}",
              "status": "receive"
            }
            ]
          }
      """
    val dummyClient = initialize(json)
    val prodBrandviewUrlPath01 = Some(prodBrandUrl01)
    val statusParam = None
    val filterParam = None
    val dependenciesParam = Some(false)
    val overviewParam = Some(false)
    val allParam = Some(false)

    val response = dummyClient.views(prodBrandviewUrlPath01, statusParam, filterParam, dependenciesParam, overviewParam, allParam)
    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map(("receive", 1))
    response.value.get.get.views.size shouldBe 1
    response.value.get.get.views(0).status shouldBe "receive"
    response.value.get.get.views(0).viewPath shouldBe prodBrandUrl01 + s"/${year}${month}${day}"
    response.value.get.get.views(0).dependencies shouldBe None
  }

  it should "correctly parse /views request and unmarshal json ProductBrand object with single view and its dependencies" in
    new schedoscopeClientTest {
    val json =
      s"""
           {
             "overview": {
               "waiting": 1,
               "materializing": 2
             },
            "views": [
            {
             "viewPath": "${pakkage}/Brand/${shop01}",
             "status": "materializing"
            },
            {
              "viewPath": "${pakkage}/Product/${shop01}/${year}/${month}/${day}/${year}${month}${day}",
              "status": "materializing"
            },
            {
             "viewPath": "${pakkage}/ProductBrand/${shop01}/${year}/${month}/${day}/${year}${month}${day}",
             "status": "waiting",
             "dependencies": {
                "dev_test_views.brand_${shop01.toLowerCase}" : ["${brandUrl01}"],
                "dev_test_views.product_${shop01.toLowerCase}" : ["${prodUrl01}/${year}${month}${day}"]
             }
            }
            ]
          }
      """
    val dummyClient = initialize(json)
    val prodBrandviewUrlPath01 = Some(prodBrandUrl01)
    val statusParam = None
    val filterParam = None
    val dependenciesParam = Some(true)
    val overviewParam = Some(false)
    val allParam = Some(false)

    val response = dummyClient.views(prodBrandviewUrlPath01, statusParam, filterParam, dependenciesParam, overviewParam, allParam)
    Await.result(response, TIMEOUT)

      response.isCompleted shouldBe true
      response.value.get.get.overview shouldBe Map(("waiting", 1), ("materializing", 2))
      response.value.get.get.views.size shouldBe 3

      response.value.get.get.views(0).status shouldBe "materializing"
      response.value.get.get.views(1).status shouldBe "materializing"
      response.value.get.get.views(2).status shouldBe "waiting"

      response.value.get.get.views(0).viewPath shouldBe brandUrl01
      response.value.get.get.views(1).viewPath shouldBe prodUrl01 + s"/${year}${month}${day}"
      response.value.get.get.views(2).viewPath shouldBe prodBrandUrl01 + s"/${year}${month}${day}"

      response.value.get.get.views(0).dependencies shouldBe None
      response.value.get.get.views(1).dependencies shouldBe None
      response.value.get.get.views(2).dependencies.get.get(s"dev_test_views.brand_${shop01.toLowerCase}") shouldBe
        Some(List(brandUrl01))
      response.value.get.get.views(2).dependencies.get.get(s"dev_test_views.product_${shop01.toLowerCase}") shouldBe
        Some(List(prodUrl01 + s"/${year}${month}${day}"))
  }

  it should "correctly parse /newdata request and unmarshal json ProductBrand object with " +
    "single view without dependencies listing" in new schedoscopeClientTest {
    val json =
      s"""
           {
             "overview": {
               "receive": 1
             },
            "views": [
            {
              "viewPath": "${pakkage}/ProductBrand/${shop01}/${year}/${month}/${day}/${year}${month}${day}",
              "status": "receive"
            }
            ]
          }
      """
    val dummyClient = initialize(json)
    val prodBrandviewUrlPath01 = Some(prodBrandUrl01)
    val statusParam = None
    val filterParam = None
    val response = dummyClient.newdata(prodBrandviewUrlPath01, statusParam, filterParam)

    Await.result(response, TIMEOUT)

    response.isCompleted shouldBe true
    response.value.get.get.overview shouldBe Map(("receive", 1))
    response.value.get.get.views.size shouldBe 1
    response.value.get.get.views(0).status shouldBe "receive"
    response.value.get.get.views(0).viewPath shouldBe prodBrandUrl01 + s"/${year}${month}${day}"
    response.value.get.get.views(0).dependencies shouldBe None
  }

  it should "correctly parse /materialize request and unmarshal json ProductBrand object with single view without dependencies" in
    new schedoscopeClientTest {
      val json =
        s"""
           {
             "overview": {
               "receive": 1
             },
            "views": [
               {
                "viewPath": "${pakkage}/ProductBrand/${shop01}/${year}/${month}/${day}/${year}${month}${day}",
                "status": "receive"
               }
            ]
          }
      """
      val dummyClient = initialize(json)
      val prodBrandviewUrlPath01 = Some(prodBrandUrl01)
      val statusParam = None
      val filterParam = None
      val modeParam = None

      val response = dummyClient.materialize(prodBrandviewUrlPath01, statusParam, filterParam, modeParam)
      Await.result(response, TIMEOUT)

      response.isCompleted shouldBe true
      response.value.get.get.overview shouldBe Map(("receive", 1))
      response.value.get.get.views.size shouldBe 1
      response.value.get.get.views(0).status shouldBe "receive"
      response.value.get.get.views(0).viewPath shouldBe prodBrandUrl01 + s"/${year}${month}${day}"
      response.value.get.get.views(0).dependencies shouldBe None
    }

  it should "correctly parse /invalidate request and unmarshal json ProductBrand object with single view without dependencies" in
    new schedoscopeClientTest {
      val json =
        s"""
           {
             "overview": {
               "invalid": 1
             },
            "views": [
               {
                "viewPath": "${pakkage}/ProductBrand/${shop01}/${year}/${month}/${day}/${year}${month}${day}",
                "status": "invalid"
               }
            ]
          }
      """
      val dummyClient = initialize(json)
      val prodBrandviewUrlPath01 = Some(prodBrandUrl01)
      val statusParam = None
      val filterParam = None
      val modeParam = None
      val response = dummyClient.invalidate(prodBrandviewUrlPath01, statusParam, filterParam, modeParam)
      Await.result(response, TIMEOUT)

      response.isCompleted shouldBe true
      response.value.get.get.overview shouldBe Map(("invalid", 1))
      response.value.get.get.views.size shouldBe 1
      response.value.get.get.views(0).status shouldBe "invalid"
      response.value.get.get.views(0).viewPath shouldBe prodBrandUrl01 + s"/${year}${month}${day}"
      response.value.get.get.views(0).dependencies shouldBe None
    }

  it should "correctly parse /transformations request and unmarshal json ProductBrand object with single view without dependencies" in
    new schedoscopeClientTest {
      val json =
        s"""
           {
             "overview": {
               "transforming": 1
             },
            "transformations": [
               {
                "actor": "${pakkage}/ProductBrand/FakeActor",
                "typ": "pig",
                "status": "transforming"
               }
            ]
          }
      """
      val dummyClient = initialize(json)
      val prodBrandviewUrlPath01 = Some(prodBrandUrl01)
      val statusParam = None
      val filterParam = None
      val modeParam = None
      val response = dummyClient.transformations(statusParam, filterParam)
      Await.result(response, TIMEOUT)

      response.isCompleted shouldBe true
      response.value.get.get.overview shouldBe Map(("transforming", 1))
      response.value.get.get.transformations.size shouldBe 1
    }

  it should "correctly parse /queues request and unmarshal json ProductBrand object with single view without dependencies" in
    new schedoscopeClientTest {
      val json =
        s"""
           {
             "overview": {
               "hive": 1,
               "oozie": 1,
               "spark": 2
             },
            "queues":
               {
                "hive": [
                  {
                    "description" : "hiveDescription",
                    "targetView" : "hiveTargetView",
                    "started" : "started",
                    "comment" : "hiveComment"
                  }
                ],
                "oozie": [
                  {
                    "description" : "oozieDescription",
                    "targetView" : "oozieTargetView",
                    "started" : "started",
                    "comment" : "oozieComment"
                  }
                ],
                "spark": [
                  {
                     "description" : "sparkDescription",
                     "targetView" : "sparkTargetView",
                     "started" : "started",
                     "comment" : "sparkComment"
                  },
                  {
                     "description" : "sparkDescription",
                     "targetView" : "sparkTargetView",
                     "started" : "started",
                     "comment" : "sparkComment"
                  }
                ]
               }
          }
      """
      val dummyClient = initialize(json)
      val prodBrandviewUrlPath01 = Some(prodBrandUrl01)
      val typParam = None
      val filterParam = None
      val response = dummyClient.queues(typParam, filterParam)
      Await.result(response, TIMEOUT)

      response.isCompleted shouldBe true
      response.value.get.get.overview shouldBe Map(("hive", 1), ("oozie", 1), ("spark", 2))
      response.value.get.get.queues.get("hive").get shouldBe List(
        RunStatus("hiveDescription","hiveTargetView","started","hiveComment",None))
      response.value.get.get.queues.get("oozie").get shouldBe List(
        RunStatus("oozieDescription","oozieTargetView","started","oozieComment",None))
      response.value.get.get.queues.get("spark").get shouldBe List(
        RunStatus("sparkDescription","sparkTargetView","started","sparkComment",None),
        RunStatus("sparkDescription","sparkTargetView","started","sparkComment",None)
      )
    }

}
