package org.schedoscope.dsl.transformations

import org.scalatest.{FlatSpec, Matchers}
import org.schedoscope.dsl.Parameter.p
import org.schedoscope.{Schedoscope, TestUtils}
import test.views.ProductBrand


class SshDistcpTransformationTest extends FlatSpec with Matchers {

  "The SshDistcpTransformation" should "generate a correct shell transformation" in {

    val settings = TestUtils.createSettings("schedoscope.development.sshTarget=bambam",
      "schedoscope.development.nameNode=feuerstein", "schedoscope.hadoop.nameNode=test")
    Schedoscope.settingsBuilder = () => settings

    val view = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))

    val sourcePath = s"hdfs://${Schedoscope.settings.prodNameNode}" +
      s"${view.fullPathBuilder(settings.prodEnv, Schedoscope.settings.prodViewDataHdfsRoot)}"

    val transformation = SshDistcpTransformation.copyFromProd(sourcePath, view, Schedoscope.settings.devSshTarget)

    //replace the dump file:/// namenode
    val res = transformation.script.replace("file:///","bambam")

    res shouldBe "ssh -K bambam 'hadoop -distcp -m 50 " +
      "hdfs://locahost:8020/hdp/prod/test/views/product_brand/shop_code=ec0106/year=2014/month=01/day=01/date_id=20140101 " +
      "hdfs://bambam/hdp/dev/test/views/product_brand/shop_code=ec0106/year=2014/month=01/day=01'"
  }

}
