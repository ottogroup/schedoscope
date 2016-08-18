/**
  * Copyright 2015 Otto (GmbH & Co KG)
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package org.schedoscope.dsl.transformations

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.schedoscope.dsl.Parameter.p
import org.schedoscope.dsl.transformations.OozieTransformation.configurationFromResource
import org.schedoscope.dsl.{Parameter, View}

case class Productfeed(ecShopCode: Parameter[String],
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
      "products_processed-oozie.bundle",
      "workflow-processed_productfeed",
      s"/hdp/${env}/applications/eci/scripts/oozie/products_processed-oozie.bundle/workflow-processed_productfeed/")
      .configureWith(
        configurationFromResource("ooziewftest.properties") ++
          Map(
            "env" -> env,
            "envDir" -> env,
            "env_dir" -> env,
            "success_flag" -> "_SUCCESS",
            "app" -> "eci",
            "output_folder" -> tablePath,
            "wtEcnr" -> ecShopCode.v.get,
            "day" -> day.v.get,
            "month" -> month.v.get,
            "year" -> year.v.get)))
}

class OozieWFTest extends FlatSpec with BeforeAndAfter with Matchers {

  "OozieWF" should "load configuration correctly" in {
    val view = Productfeed(p("ec0101"), p("2014"), p("10"), p("11"))

    val t = view.transformation().asInstanceOf[OozieTransformation]

    t.workflowAppPath shouldEqual "/hdp/dev/applications/eci/scripts/oozie/products_processed-oozie.bundle/workflow-processed_productfeed/"

    val expectedConfiguration = Map(
      "oozie.bundle.application.path" -> "${nameNode}${bundlePath}",
      "oozie.use.system.libpath" -> true,
      "preprocMrIndir" -> "${stageDir}/preproc-in/${wtEcnr}/",
      "datahubBaseDir" -> "${nameNode}/hdp/${envDir}/applications/eci/datahub",
      "output_folder" -> "/hdp/dev/org/schedoscope/dsl/transformations/productfeed",
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
