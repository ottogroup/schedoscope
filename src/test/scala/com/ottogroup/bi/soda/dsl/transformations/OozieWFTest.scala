package com.ottogroup.bi.soda.dsl.transformations

import org.scalatest.BeforeAndAfter
import org.scalatest.Matchers
import org.scalatest.FlatSpec
import com.ottogroup.bi.soda.dsl.Parameter
import com.ottogroup.bi.soda.dsl.Parameter._
import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.dsl.Parquet
import com.ottogroup.bi.soda.dsl.transformations.oozie.OozieTransformation
import com.ottogroup.bi.soda.dsl.transformations.oozie.OozieTransformation._

case class Productfeed(
  ecShopCode: Parameter[String],
  year: Parameter[String],
  month: Parameter[String],
  day: Parameter[String]) extends View {

  val artNumber = fieldOf[String]
  val artName = fieldOf[String]
  val imageUrl = fieldOf[String]
  val category = fieldOf[String]
  val gender = fieldOf[String]
  val brand = fieldOf[String]

  transformVia(() =>
    OozieTransformation(
      "products_processed-bundle",
      "workflow-processed_productfeed",
      s"/hdp/${env}/applications/eci/scripts/oozie/products_processed-bundle/workflow-processed_productfeed/",
      configurationFromResource("test.properties") ++
        Map(
          "env" -> env,
          "envDir" -> env,
          "env_dir" -> env,
          "success_flag" -> "_SUCCESS",
          "app" -> "eci",
          "output_folder" -> locationPath,
          "wtEcnr" -> ecShopCode.v.get,
          "day" -> day.v.get,
          "month" -> month.v.get,
          "year" -> year.v.get)))
}

class OozieWFTest extends FlatSpec with BeforeAndAfter with Matchers {

  "OozieWF" should "load configuration correctly" in {
    val view = Productfeed(p("ec0101"), p("2014"), p("10"), p("11"))

    val t = view.transformation().asInstanceOf[OozieTransformation]

    t.workflowAppPath shouldEqual "/hdp/dev/applications/eci/scripts/oozie/products_processed-bundle/workflow-processed_productfeed/"

    val expectedConfiguration = Map(
      "oozie.bundle.application.path" -> "${nameNode}${bundlePath}",
      "oozie.use.system.libpath" -> true,
      "preprocMrIndir" -> "${stageDir}/preproc-in/${wtEcnr}/",
      "datahubBaseDir" -> "${nameNode}/hdp/${envDir}/applications/eci/datahub",
      "output_folder" -> "/hdp/dev/com/ottogroup/bi/soda/dsl/transformations/productfeed",
      "preprocMrOutdir" -> "${stageDir}/preproc-out/${wtEcnr}/",
      "preprocOrigDir" -> "${incomingDir}/",
      "year" -> "2014",
      "preprocTmpDir" -> "${stageDir}/preprocessed/webtrends_log_${wtEcnr}",
      "success_flag" -> "_SUCCESS",
      "sessionDir" -> "${datahubBaseDir}/sessions/${wtEcnr}",
      "envDir" -> "dev",
      "wtEcnr" -> "ec0101",
      "env_dir" -> "dev",
      "incomingDir" -> "${stageDir}/${wtEcnr}",
      "app" -> "eci",
      "preprocOutfilePrefix" -> "${loop_datum}-preprocessed",
      "oozieLauncherQueue" -> "root.webtrends-oozie-launcher",
      "throttle" -> "200",
      "processedDir" -> "${nameNode}/hdp/${envDir}/applications/eci/processed",
      "timeout" -> "10080",
      "stageDir" -> "${nameNode}/hdp/${envDir}/applications/eci/stage",
      "day" -> "11",
      "env" -> "dev",
      "month" -> "10")

    t.configuration.foreach { case (key, value) => expectedConfiguration(key).toString shouldEqual value.toString }
  }
}