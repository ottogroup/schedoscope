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

import java.util.Date

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.schedoscope.dsl.Parameter.p
import org.schedoscope.dsl.transformations.HiveTransformation.queryFromResource
import org.schedoscope.dsl.transformations.Transformation.replaceParameters
import org.schedoscope.dsl.{Parameter, Structure, View}
import org.schedoscope.schema.ddl.HiveQl
import test.views._


case class Article() extends Structure {
  val name = fieldOf[String]
  val number = fieldOf[Int]
}

case class OrderItem(year: Parameter[Int], month: Parameter[Int], day: Parameter[Int]) extends View {
  val orderId = fieldOf[Int]
  val pos = fieldOf[Int]
  val article = fieldOf[Article]
  val price = fieldOf[Float]
  val eans = fieldOf[List[String]]
}

case class Order(year: Parameter[Int], month: Parameter[Int], day: Parameter[Int]) extends View {
  val viewId = fieldOf[Int]
  val date = fieldOf[Date]
  val customerNumber = fieldOf[String]
}

case class OrderAll(year: Parameter[Int], month: Parameter[Int], day: Parameter[Int]) extends View {
  val viewId = fieldOf[Int]
  val date = fieldOf[Date]
  val customerNumber = fieldOf[String]
  val pos = fieldOf[Int]
  val article = fieldOf[String]
  val number = fieldOf[Int]
  val price = fieldOf[Float]

  val orderItem = dependsOn(() => OrderItem(year, month, day))
  val order = dependsOn(() => Order(year, month, day))
}

class HiveQlTest extends FlatSpec with BeforeAndAfter with Matchers {

  it should "generate Parquet row format and tblproperties sql statement" in {
    val view = ArticleViewParquet()
    HiveQl.ddl(view) shouldEqual
      """	CREATE EXTERNAL TABLE IF NOT EXISTS dev_test_views.article_view_parquet (
        |		name STRING,
        |		number INT
        |	)
        |	STORED AS PARQUET
        |	TBLPROPERTIES (
        |		 'orc.compress' = 'ZLIB',
        |		 'transactional' = 'true'
        |	)
        |	LOCATION '/hdp/dev/test/views/article_view_parquet'
        | """.stripMargin
  }

  it should "generate Parquet row format sql statement" in {
    val view = ArticleViewParquet2()
    HiveQl.ddl(view) shouldEqual
      """	CREATE EXTERNAL TABLE IF NOT EXISTS dev_test_views.article_view_parquet2 (
        |		name STRING,
        |		number INT
        |	)
        |	ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        |	STORED AS
        |		INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        |		OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        |	LOCATION '/hdp/dev/test/views/article_view_parquet2'
        | """.stripMargin
  }

  it should "generate Sequence file row format" in {
    val view = ArticleViewSequence()
    HiveQl.ddl(view) shouldEqual
      """	CREATE EXTERNAL TABLE IF NOT EXISTS dev_test_views.article_view_sequence (
        |		name STRING,
        |		number INT
        |	)
        |	STORED AS SEQUENCEFILE
        |	LOCATION '/hdp/dev/test/views/article_view_sequence'
        | """.stripMargin
  }

  it should "generate avro row format and tblproperties sql statement" in {
    val view = ArticleViewAvro()
    val hack = ""
    HiveQl.ddl(view) shouldEqual
      s"""	CREATE EXTERNAL TABLE IF NOT EXISTS dev_test_views.article_view_avro ${hack}
          |	ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
          |	WITH SERDEPROPERTIES (
          |		 'avro.schema.url' = 'hdfs:///hdp/dev/global/datadictionary/schema/avro/myPath'
          |	)
          |	STORED AS
          |		INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
          |		OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
          |	TBLPROPERTIES (
          |		 'immutable' = 'true'
          |	)
          |	LOCATION '/hdp/dev/test/views/article_view_avro'
          |""".stripMargin
  }

  it should "generate avro consise format" in {
    val view = ArticleViewAvro2()
    val hack = ""
    HiveQl.ddl(view) shouldEqual
      s"""	CREATE EXTERNAL TABLE IF NOT EXISTS dev_test_views.article_view_avro2 ${hack}
          |	STORED AS AVRO
          |	TBLPROPERTIES (
          |		 'immutable' = 'true'
          |	)
          |	LOCATION '/hdp/dev/test/views/article_view_avro2'
          |""".stripMargin
  }

  it should "generate ORC row format and tblproperties sql statement" in {
    val view = ArticleViewOrc()
    HiveQl.ddl(view) shouldEqual
      """	CREATE EXTERNAL TABLE IF NOT EXISTS dev_test_views.article_view_orc (
        |		name STRING,
        |		number INT
        |	)
        |	STORED AS ORC
        |	TBLPROPERTIES (
        |		 'immutable' = 'false'
        |	)
        |	LOCATION '/hdp/dev/test/views/article_view_orc'
        | """.stripMargin
  }

  it should "generate ORC row format sql statement" in {
    val view = ArticleViewOrc2()
    HiveQl.ddl(view) shouldEqual
      """	CREATE EXTERNAL TABLE IF NOT EXISTS dev_test_views.article_view_orc2 (
        |		name STRING,
        |		number INT
        |	)
        |	ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
        |	STORED AS
        |		INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
        |		OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
        |	LOCATION '/hdp/dev/test/views/article_view_orc2'
        | """.stripMargin
  }

  it should "generate TextFile row format and tblproperties sql statement" in {
    val view = ArticleViewTextFile1()
    HiveQl.ddl(view) shouldEqual
      """	CREATE EXTERNAL TABLE IF NOT EXISTS dev_test_views.article_view_text_file1 (
        |		name STRING,
        |		number INT
        |	)
        |	ROW FORMAT DELIMITED
        |	FIELDS TERMINATED BY '\\001'
        |	LINES TERMINATED BY '\n'
        |	COLLECTION ITEMS TERMINATED BY '\002'
        |	MAP KEYS TERMINATED BY '\003'
        |	STORED AS TEXTFILE
        |	TBLPROPERTIES (
        |		 'what' = 'ever'
        |	)
        |	LOCATION '/hdp/dev/test/views/article_view_text_file1'
        | """.stripMargin
  }

  it should "generate TextFile2 row format and tblproperties sql statement" in {
    val view = ArticleViewTextFile2()
    HiveQl.ddl(view) shouldEqual
      """	CREATE EXTERNAL TABLE IF NOT EXISTS dev_test_views.article_view_text_file2 (
        |		name STRING,
        |		number INT
        |	)
        |	ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        |	WITH SERDEPROPERTIES (
        |		 'separatorChar' = '\t',
        |		 'escapeChar' = '\\'
        |	)
        |	STORED AS TEXTFILE
        |	TBLPROPERTIES (
        |		 'what' = 'buh'
        |	)
        |	LOCATION '/hdp/dev/test/views/article_view_text_file2'
        | """.stripMargin
  }

  it should "generate TextFile3 in row format" in {
    val view = ArticleViewTextFile3()
    HiveQl.ddl(view) shouldEqual
      """	CREATE EXTERNAL TABLE IF NOT EXISTS dev_test_views.article_view_text_file3 (
        |		name STRING,
        |		number INT
        |	)
        |	STORED AS
        |		INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
        |		OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
        |	TBLPROPERTIES (
        |		 'what' = 'buh'
        |	)
        |	LOCATION '/hdp/dev/test/views/article_view_text_file3'
        | """.stripMargin
  }

  it should "generate Record Columnar format and tblproperties sql statement" in {
    val view = ArticleViewRc()
    HiveQl.ddl(view) shouldEqual
      """	CREATE EXTERNAL TABLE IF NOT EXISTS dev_test_views.article_view_rc (
        |		name STRING,
        |		number INT
        |	)
        |	STORED AS RCFILE
        |	TBLPROPERTIES (
        |		 'scalable' = 'true'
        |	)
        |	LOCATION '/hdp/dev/test/views/article_view_rc'
        | """.stripMargin
  }

  it should "generate json row format and tblproperties sql statement" in {
    val view = ArticleViewJson()
    HiveQl.ddl(view) shouldEqual
      """	CREATE EXTERNAL TABLE IF NOT EXISTS dev_test_views.article_view_json (
        |		name STRING,
        |		number INT
        |	)
        |	ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
        |	STORED AS TEXTFILE
        |	TBLPROPERTIES (
        |		 'transactional' = 'true'
        |	)
        |	LOCATION '/hdp/dev/test/views/article_view_json'
        | """.stripMargin
  }

  it should "generate custom serde with properties and stored as textfile" in {
    val view = ArticleViewCsv()
    HiveQl.ddl(view) shouldEqual
      """	CREATE EXTERNAL TABLE IF NOT EXISTS dev_test_views.article_view_csv (
        |		name STRING,
        |		number INT
        |	)
        |	ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        |	WITH SERDEPROPERTIES (
        |		 'separatorChar' = '\t',
        |		 'quoteChar' = ''',
        |		 'escapeChar' = '\\'
        |	)
        |	STORED AS TEXTFILE
        |	LOCATION '/hdp/dev/test/views/article_view_csv'
        | """.stripMargin
  }

  it should "generate custom RegEx serde with properties and stored as textfile" in {
    val view = ArticleViewRegEx()
    HiveQl.ddl(view) shouldEqual
      """	CREATE EXTERNAL TABLE IF NOT EXISTS dev_test_views.article_view_reg_ex (
        |		name STRING,
        |		number INT
        |	)
        |	ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
        |	WITH SERDEPROPERTIES (
        |		 'input.regex' = 'test'
        |	)
        |	STORED AS TEXTFILE
        |	LOCATION '/hdp/dev/test/views/article_view_reg_ex'
        | """.stripMargin
  }

  it should "generate inputOutput row format and tblproperties sql statement" in {
    val view = ArticleViewInOutput()
    HiveQl.ddl(view) shouldEqual
      """	CREATE EXTERNAL TABLE IF NOT EXISTS dev_test_views.article_view_in_output (
        |		name STRING,
        |		number INT
        |	)
        |	ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
        |	STORED AS
        |		INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
        |		OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
        |	TBLPROPERTIES (
        |		 'EXTERNAL' = 'TRUE'
        |	)
        |	LOCATION '/hdp/dev/test/views/article_view_in_output'
        | """.stripMargin
  }

  it should "generate S3 row format and tblproperties sql statement" in {
    val view = ArticleViewS3()
    HiveQl.ddl(view) shouldEqual
      """	CREATE EXTERNAL TABLE IF NOT EXISTS dev_test_views.article_view_s3 (
        |		name STRING,
        |		number INT
        |	)
        |	STORED AS ORC
        |	TBLPROPERTIES (
        |		 'orc.compress' = 'ZLIB',
        |		 'transactional' = 'true'
        |	)
        |	LOCATION 's3a://schedoscope-bucket-test/dev/test/views/article_view_s3'
        | """.stripMargin
  }

  "HiveTransformation.insertInto" should "generate correct static partitioning prefix by default" in {
    val orderAll = OrderAll(p(2014), p(10), p(12))

    HiveTransformation.insertInto(orderAll, "SELECT * FROM STUFF") shouldEqual
      """INSERT OVERWRITE TABLE dev_org_schedoscope_dsl_transformations.order_all
PARTITION (year = '2014', month = '10', day = '12')
SELECT * FROM STUFF"""
  }

  it should "not generate a partitioning prefix if requested" in {
    val orderAll = OrderAll(p(2014), p(10), p(12))

    HiveTransformation.insertInto(
      orderAll,
      "SELECT * FROM STUFF",
      partition = false) shouldEqual
      """INSERT OVERWRITE TABLE dev_org_schedoscope_dsl_transformations.order_all
SELECT * FROM STUFF"""
  }

  it should "generate settings if needed" in {
    val orderAll = OrderAll(p(2014), p(10), p(12))

    HiveTransformation.insertInto(
      orderAll,
      "SELECT * FROM STUFF",
      settings = Map(
        "parquet.compression" -> "GZIP",
        "my.setting" -> "true")) shouldEqual
      """SET parquet.compression=GZIP;
SET my.setting=true;
INSERT OVERWRITE TABLE dev_org_schedoscope_dsl_transformations.order_all
PARTITION (year = '2014', month = '10', day = '12')
SELECT * FROM STUFF"""
  }

  "HiveTransformation.insertDynamicallyInto" should "generate correct dynamic partitioning prefix by default" in {
    val orderAll = OrderAll(p(2014), p(10), p(12))

    HiveTransformation.insertDynamicallyInto(
      orderAll,
      "SELECT * FROM STUFF") shouldEqual
      """SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
INSERT OVERWRITE TABLE dev_org_schedoscope_dsl_transformations.order_all
PARTITION (year, month, day)
SELECT * FROM STUFF"""
  }

  "HiveTransformation.replaceParameters" should "replace parameter placeholders" in {
    replaceParameters("${a} ${a} ${b}", Map("a" -> "A", "b" -> Boolean.box(true))) shouldEqual ("A A true")
  }

  it should "quote semicolons with $-parameters" in {
    replaceParameters("SELECT * FROM test WHERE id = ${a}", Map("a" -> "1; DROP TABLE")) shouldEqual "SELECT * FROM test WHERE id = 1\\; DROP TABLE"
  }

  it should "quote \" with $-parameters" in {
    replaceParameters("SELECT * FROM test WHERE id = \"${a}\"", Map("a" -> "1\"; DROP TABLE test;\"x")) shouldEqual "SELECT * FROM test WHERE id = \"1\\\"\\; DROP TABLE test\\;\\\"x\""
  }

  it should "quote ' with $-parameters" in {
    replaceParameters("SELECT * FROM test WHERE id = '${a}'", Map("a" -> "1'; DROP TABLE test;'x")) shouldEqual "SELECT * FROM test WHERE id = '1\\'\\; DROP TABLE test\\;\\'x'"
  }

  it should "quote semicolons with §-parameters" in {
    replaceParameters("SELECT * FROM test WHERE id = §{a}", Map("a" -> "1; DROP TABLE")) shouldEqual "SELECT * FROM test WHERE id = 1\\; DROP TABLE"
  }

  it should "not quote \" with §-parameters" in {
    replaceParameters("SELECT * FROM test WHERE id = \"§{a}\"", Map("a" -> "1\"; DROP TABLE test;\"x")) shouldEqual "SELECT * FROM test WHERE id = \"1\"\\; DROP TABLE test\\;\"x\""
  }

  it should "not quote ' with §-parameters" in {
    replaceParameters("SELECT * FROM test WHERE id = '§{a}'", Map("a" -> "1'; DROP TABLE test;'x")) shouldEqual "SELECT * FROM test WHERE id = '1'\\; DROP TABLE test\\;'x'"
  }


  "HiveTransformation.queryFrom" should "read queries from external file" in {
    val orderAll = OrderAll(p(2014), p(10), p(12))
    HiveTransformation.insertInto(
      orderAll,
      replaceParameters(
        queryFromResource("test.sql"),
        Map(
          "param" -> Int.box(2),
          "anotherParam" -> "Value")),
      settings = Map(
        "parquet.compression" -> "GZIP",
        "my.setting" -> "true")) shouldEqual
      """SET parquet.compression=GZIP;
SET my.setting=true;
INSERT OVERWRITE TABLE dev_org_schedoscope_dsl_transformations.order_all
PARTITION (year = '2014', month = '10', day = '12')
SELECT *
FROM STUFF
WHERE param = 2
AND anotherParam = 'Value'"""
  }

  "the normalize query function" should "delete comments" in {
    val qry = "-----23123-xcvo\tn .cvaotrsaooiarst9210132q56`]-[ taroistneaorsitdthdastrtsra\n" +
      "--a comment\n" +
      "\\--no comment\n" +
      "--\n" +
      "select * from coolstuff\n" +
      "LIMIT 10"

    HiveTransformation.normalizeQuery(qry) shouldBe "\\--no comment select * from coolstuff LIMIT 10"
  }

  it should "remove whitespaces from the start and end of lines" in {
    val qry = "   \t\t\t    test \t \n" +
      "  \t \tte\tst    \n" +
      "wh at\t   "

    HiveTransformation.normalizeQuery(qry) shouldBe "test te st wh at"
  }

  it should "remove set commands" in {
    val qry = "NOSET key=value;\n" +
      "SET test=hello; \n" +
      "set test=hi; \n" +
      "set settings=hi; \n" +
      "SET test=key; SET hello=world;\n" +
      "SET test=key; SET hello=world; hello\n" +
      "SELECT;"

    HiveTransformation.normalizeQuery(qry) shouldBe "NOSET key=value; hello SELECT;"
  }

  it should "remove duplicate whitespaces" in {
    val qry1 = "\"this  is\"   a\ttest"
    val qry2 = "'this  is'   a\ttest"
    val qry3 = "'  '  '  ' "
    val qry4 = "'  \\'  '  '  \\'  ' "

    HiveTransformation.normalizeQuery(qry1) shouldBe "\"this;;is\" a test"
    HiveTransformation.normalizeQuery(qry2) shouldBe "'this;;is' a test"
    HiveTransformation.normalizeQuery(qry3) shouldBe "';;' ';;'"
    HiveTransformation.normalizeQuery(qry4) shouldBe "';;\\';;' ';;\\';;'"
  }

  it should "normalize two queries to be equal" in {
    val qry1 = "SET memory=1024;\n" +
      "SET date='20160412';\n" +
      "\n" +
      "SELECT \n" +
      "  price \n" +
      "FROM \n" +
      "  transactions\n" +
      "WHERE\n" +
      "  date='${date}'"

    val qry2 = "SET memory=2028;\n" +
      "SET date='20160413';\n" +
      "\n" +
      "\n" +
      "SELECT price \n" +
      "FROM transactions\n" +
      "WHERE date='${date}'"

    HiveTransformation.normalizeQuery(qry1) should
      equal(HiveTransformation.normalizeQuery(qry2))
  }

  it should "not normalize two different queries to be equal" in {
    val qry1 = "SET memory=1024;\n" +
      "SET date='20160412';\n" +
      "\n" +
      "SELECT \n" +
      "  price \n" +
      "FROM \n" +
      "  transactions\n" +
      "WHERE\n" +
      "  date='${date}'"

    val qry2 = "SET memory=2028;\n" +
      "SET date='20160413';\n" +
      "\n" +
      "\n" +
      "SELECT price \n" +
      "FROM transactions\n" +
      //use '<>' instead of '='
      "WHERE date<>'${date}'"

    HiveTransformation.normalizeQuery(qry1) should not
    equal(HiveTransformation.normalizeQuery(qry2))
  }

  "replace Whitespaces in quotes" should "do it's thing" in {
    val replaceDQ = HiveTransformation.replaceWhitespacesBetweenChars("\"") _
    replaceDQ("\"") shouldBe "\""
    replaceDQ("\"\"") shouldBe "\"\""
    replaceDQ("test") shouldBe "test"
    replaceDQ("te\"st") shouldBe "te\"st"
    replaceDQ("\"  \"") shouldBe "\";;\""
    replaceDQ("\"  \"\\\"") shouldBe "\";;\"\\\""
    replaceDQ("\"  \" \" \"") shouldBe "\";;\" \";\""
    replaceDQ("hi\"  \" \" \"") shouldBe "hi\";;\" \";\""
    replaceDQ("hi\"  \" test \" \"hi") shouldBe "hi\";;\" test \";\"hi"
    replaceDQ("\" te\\\"st \"") shouldBe "\";te\\\"st;\""
    replaceDQ("\" te\\\"st\\\" \"") shouldBe "\";te\\\"st\\\";\""
    replaceDQ("\" \\\"") shouldBe "\" \\\""
    replaceDQ("\\\" \"") shouldBe "\\\" \""
    replaceDQ("\" \\\"s") shouldBe "\" \\\"s"
    replaceDQ("\\\" \"s") shouldBe "\\\" \"s"
    replaceDQ("\"\" \" \"s") shouldBe "\"\" \";\"s"

  }

  it should "work when nested" in {
    val replaceDQ = HiveTransformation.replaceWhitespacesBetweenChars("\"") _
    val replaceSQ = HiveTransformation.replaceWhitespacesBetweenChars("'") _

    val replace = (s: String) => replaceSQ(replaceDQ(s))

    replace("\"  \"") shouldBe "\";;\""
    replace("\"  \"\\\"") shouldBe "\";;\"\\\""
    replace("\"  \" ' '") shouldBe "\";;\" ';'"
    replace("hi\"  \" \" \"") shouldBe "hi\";;\" \";\""
    replace("\" \' \" \'") shouldBe "\";\";\" '"
    replace("\" \' \' \"") shouldBe "\";\";\";\""
    replace("\" \\' \" \\'") shouldBe "\";\\\";\" \\'"
    replace("\" ' \" '") shouldBe "\";\";\" '"
  }

  "Hivetransformation checksum" should "apply the normalize function" in {

    val settings = Map(
      "memory" -> "12",
      "date" -> "20160102")

    val orderAll = OrderAll(p(2014), p(10), p(12))

    val hiveTransformation = new HiveTransformation(
      HiveTransformation.insertInto(
        orderAll,
        "select * from\t\nprices  where id = '12  23'",
        false,
        settings)
      , List()
    )

    hiveTransformation.stringsToChecksum shouldBe
      List("INSERT OVERWRITE TABLE dev_org_schedoscope_dsl_transformations.order_all " +
        "select * from prices where id = '12;;23'")
  }

  "Hivetransformation.validateQuery" should "find uneven amount of joins and ons" in {
    val qry1 = "SELECT JOIN ON TEST"
    val qry2 = "SELECT join on TEST"
    val qry3 = "SELECT 'join' on JOIN"
    val qry4 = "JOIN ON"
    val qry5 = "ON"
    val qry6 = "JOIN"
    val qry7 = "JOIN ON ON"
    val qry8 = "JOIN JOIN ON ON"
    val qry9 = "SELECT \"join\" on JOIN"
    val qry10 = "SELECT \\\"join\" on JOIN"
    val qry11 = "SELECT * '' JOIN 'test' ON"
    val qry12 = "SELECT * FROM joined JOIN onner ON stuff"
    val qry13 = "SELECT * FROM joined JOIN( SELECT ) x ON x.stuff"
    val qry14 = "SELECT * FROM joined CROSS JOIN select!"

    HiveTransformation.checkJoinsWithOns(qry1) shouldBe true
    HiveTransformation.checkJoinsWithOns(qry2) shouldBe true
    HiveTransformation.checkJoinsWithOns(qry3) shouldBe true
    HiveTransformation.checkJoinsWithOns(qry4) shouldBe true
    HiveTransformation.checkJoinsWithOns(qry5) shouldBe false
    HiveTransformation.checkJoinsWithOns(qry6) shouldBe false
    HiveTransformation.checkJoinsWithOns(qry7) shouldBe false
    HiveTransformation.checkJoinsWithOns(qry8) shouldBe true
    HiveTransformation.checkJoinsWithOns(qry9) shouldBe true
    HiveTransformation.checkJoinsWithOns(qry10) shouldBe true
    HiveTransformation.checkJoinsWithOns(qry11) shouldBe true
    HiveTransformation.checkJoinsWithOns(qry12) shouldBe true
    HiveTransformation.checkJoinsWithOns(qry13) shouldBe true
    HiveTransformation.checkJoinsWithOns(qry14) shouldBe true
  }

}
