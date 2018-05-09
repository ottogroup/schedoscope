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

import java.sql.DriverManager
import java.util.Properties

import _root_.test.views._
import org.apache.commons.net.ftp.FTPClient
import org.apache.curator.test.TestingServer
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.map.`type`.TypeFactory
import org.rarefiedredis.redis.adapter.jedis.JedisAdapter
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.schedoscope.Schedoscope
import org.schedoscope.dsl.Field.v
import org.schedoscope.dsl.Parameter.p
import org.schedoscope.export.testsupport.{EmbeddedFtpSftpServer, EmbeddedKafkaCluster, SimpleTestKafkaConsumer}
import org.schedoscope.export.utils.BigQueryUtils.{bigQueryService, dropDataset, existsDataset}
import org.schedoscope.export.utils.CloudStorageUtils.{createBucket, deleteBucket, storageService}
import org.schedoscope.export.utils.RedisMRJedisFactory
import org.schedoscope.test.{rows, test}

import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.collection.JavaConverters._

class ExportTest extends FlatSpec with Matchers with BeforeAndAfter {

  private val CALL_BIG_QUERY = true
  private val CLEAN_UP_BIG_QUERY = false

  before {
    if (CALL_BIG_QUERY) {
      val bigQuery = bigQueryService
      val storage = storageService

      if (existsDataset(bigQuery, null, "default"))
        dropDataset(bigQuery, null, "default")

      createBucket(storage, "schedoscope_export_big_query_full_test", "europe-west3")
    }
  }

  after {
    if (CALL_BIG_QUERY && CLEAN_UP_BIG_QUERY) {
      val bigQuery = bigQueryService
      val storage = storageService

      if (existsDataset(bigQuery, null, "default"))
        dropDataset(bigQuery, null, "default")

      deleteBucket(storage, "schedoscope_export_big_query_full_test")
    }
  }

  Class.forName("org.apache.derby.jdbc.EmbeddedDriver")
  val dbConnection = DriverManager.getConnection("jdbc:derby:memory:TestingDB;create=true")

  val jedisAdapter = new JedisAdapter()
  RedisMRJedisFactory.setJedisMock(jedisAdapter)


  val ec0101Clicks = new Click(p("EC0101"), p("2014"), p("01"), p("01")) with rows {
    set(
      v(id, "event01"),
      v(url, "http://ec0101.com/url1"))
    set(
      v(id, "event02"),
      v(url, "http://ec0101.com/url2"))
    set(
      v(id, "event03"),
      v(url, "http://ec0101.com/url3"))
  }

  val ec0106Clicks = new Click(p("EC0106"), p("2014"), p("01"), p("01")) with rows {
    set(
      v(id, "event04"),
      v(url, "http://ec0106.com/url1"))
    set(
      v(id, "event05"),
      v(url, "http://ec0106.com/url2"))
    set(
      v(id, "event06"),
      v(url, "http://ec0106.com/url3"))
  }

  "The test framework" should "execute hive transformations and perform JDBC export" in {

    new ClickOfEC0101WithJdbcExport(p("2014"), p("01"), p("01")) with test {
      basedOn(ec0101Clicks, ec0106Clicks)

      `then`()

      numRows shouldBe 3

      row(
        v(id) shouldBe "event01",
        v(url) shouldBe "http://ec0101.com/url1")
      row(
        v(id) shouldBe "event02",
        v(url) shouldBe "http://ec0101.com/url2")
      row(
        v(id) shouldBe "event03",
        v(url) shouldBe "http://ec0101.com/url3")

    }

    val statement = dbConnection.createStatement()
    val resultSet = statement.executeQuery("SELECT COUNT(*) FROM DEV_TEST_VIEWS_CLICK_OF_E_C0101_WITH_JDBC_EXPORT")
    resultSet.next()

    resultSet.getInt(1) shouldBe 3

    resultSet.close()
    statement.close()
  }

  it should "execute hive transformations and perform BigQuery export" in {

    if (CALL_BIG_QUERY)
      new ClickOfEC0101WithBigQueryExport(p("2014"), p("01"), p("01")) with test {
        basedOn(ec0101Clicks, ec0106Clicks)

        `then`()

        numRows shouldBe 3

        row(
          v(id) shouldBe "event01",
          v(url) shouldBe "http://ec0101.com/url1")
        row(
          v(id) shouldBe "event02",
          v(url) shouldBe "http://ec0101.com/url2")
        row(
          v(id) shouldBe "event03",
          v(url) shouldBe "http://ec0101.com/url3")

      }
  }

  it should "execute hive transformations and perform Redis export" in {

    new ClickOfEC0101WithRedisExport(p("2014"), p("01"), p("01")) with test {
      basedOn(ec0101Clicks, ec0106Clicks)

      `then`()

      numRows shouldBe 3

      row(
        v(id) shouldBe "event01",
        v(url) shouldBe "http://ec0101.com/url1")
      row(
        v(id) shouldBe "event02",
        v(url) shouldBe "http://ec0101.com/url2")
      row(
        v(id) shouldBe "event03",
        v(url) shouldBe "http://ec0101.com/url3")

    }

    jedisAdapter.hget("event01", "url") shouldBe "http://ec0101.com/url1"
    jedisAdapter.hget("event02", "url") shouldBe "http://ec0101.com/url2"
    jedisAdapter.hget("event03", "url") shouldBe "http://ec0101.com/url3"

  }

  it should "execute hive transformations and perform Kafka export" in {

    val zkServer = new TestingServer(2182);
    zkServer.start()
    Thread.sleep(500)

    val kafkaServer = new EmbeddedKafkaCluster(zkServer.getConnectString, new Properties(), List(new Integer(9092)).asJava)
    kafkaServer.startup()

    val v = new ClickOfEC01WithKafkaExport(p("2014"), p("01"), p("01")) with test {
      basedOn(ec0101Clicks, ec0106Clicks)

      `then`()

      numRows shouldBe 3

      row(
        v(id) shouldBe "event01",
        v(url) shouldBe "http://ec0101.com/url1")
      row(
        v(id) shouldBe "event02",
        v(url) shouldBe "http://ec0101.com/url2")
      row(
        v(id) shouldBe "event03",
        v(url) shouldBe "http://ec0101.com/url3")

    }

    val consumer = new SimpleTestKafkaConsumer(v.dbName + "_" + v.n, zkServer.getConnectString, 3)
    for (r <- consumer) {
      val record: java.util.HashMap[String, _] = new ObjectMapper().readValue(r, TypeFactory.mapType(classOf[java.util.HashMap[_, _]], classOf[String], classOf[Any]))
      record.get("date_id") shouldBe "20140101"
    }

    kafkaServer.shutdown()
    zkServer.stop()

  }

  it should "execute hive transformations and perform Ftp export" in {

    val ftpServer = new EmbeddedFtpSftpServer()
    ftpServer.startEmbeddedFtpServer()

    val v = new ClickOfEC0101WithFtpExport(p("2014"), p("01"), p("01")) with test {
      basedOn(ec0101Clicks, ec0106Clicks)

      `then`()

      numRows shouldBe 3

      row(
        v(id) shouldBe "event01",
        v(url) shouldBe "http://ec0101.com/url1")
      row(
        v(id) shouldBe "event02",
        v(url) shouldBe "http://ec0101.com/url2")
      row(
        v(id) shouldBe "event03",
        v(url) shouldBe "http://ec0101.com/url3")

    }

    val ftp = new FTPClient();
    ftp.connect("localhost", 2221);
    ftp.login(EmbeddedFtpSftpServer.FTP_USER_FOR_TESTING, EmbeddedFtpSftpServer.FTP_PASS_FOR_TESTING);
    val files = ftp.listFiles();

    files.filter {
      _.getName().contains(v.filePrefix)
    }.length shouldBe Schedoscope.settings.ftpExportNumReducers

    ftpServer.stopEmbeddedFtpServer()
  }
}