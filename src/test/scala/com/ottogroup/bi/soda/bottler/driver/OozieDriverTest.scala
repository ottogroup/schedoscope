package com.ottogroup.bi.soda.bottler.driver

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import com.ottogroup.bi.soda.test.resources.LocalTestResources
import com.ottogroup.bi.soda.test.resources.LocalTestResources
import com.ottogroup.bi.soda.dsl.transformations.sql.HiveTransformation
import com.ottogroup.bi.soda.test.resources.OozieTestResources
import com.ottogroup.bi.soda.test.resources.TestResources
import com.ottogroup.bi.soda.dsl.transformations.oozie.OozieTransformation
import org.apache.hadoop.fs.Path
import java.net.URI

class OozieDriverTest extends FlatSpec with Matchers {

  var cachedResources: OozieTestResources = null
  def resources: OozieTestResources = {
    if (cachedResources == null)
      cachedResources = new OozieTestResources()
    cachedResources
  }

  def cluster = resources.mo
  def driver = resources.oozieDriver

  def deployWorkflow(wf: OozieTransformation) = {
    val hdfs = resources.fileSystem
    val dest = new Path(s"${resources.namenode}/${wf.workflowAppPath}")
    val src = new Path(s"src/test/resources/${wf.bundle}/${wf.workflow}/workflow.xml")

    println(s"Uploading workflow ${wf.workflow} from ${src} to ${dest}")

    if (!hdfs.exists(dest))
      hdfs.mkdirs(dest)

    hdfs.copyFromLocalFile(src, dest)
  }

  var cachedWorkingOozieTransformation: OozieTransformation = null
  def workingOozieTransformation = {
    if (cachedWorkingOozieTransformation == null) {
      cachedWorkingOozieTransformation = OozieTransformation(
        "bundle", "workflow",
        "/tmp/soda/oozie/workflows/bundle/workflow/", Map(
          "jobTracker" -> cluster.getJobTrackerUri(),
          "nameNode" -> cluster.getNameNodeUri(),
          "oozie.use.system.libpath" -> "false"))

      deployWorkflow(cachedWorkingOozieTransformation)
    }

    cachedWorkingOozieTransformation
  }

  var cachedFailingOozieTransformation: OozieTransformation = null
  def failingOozieTransformation = {
    if (cachedFailingOozieTransformation == null) {
      cachedFailingOozieTransformation = OozieTransformation(
        "bundle", "failflow",
        "/tmp/soda/oozie/workflows/bundle/failflow/", Map(
          "jobTracker" -> cluster.getJobTrackerUri(),
          "nameNode" -> cluster.getNameNodeUri(),
          "oozie.use.system.libpath" -> "false"))

      deployWorkflow(cachedFailingOozieTransformation)
    }

    cachedFailingOozieTransformation
  }

  "Oozie" should "be named oozie" taggedAs (DriverTests, OozieTests) in {
    driver.name shouldBe "oozie"
  }

  it should "execute oozie tranformations synchronously" taggedAs (DriverTests, OozieTests) in {
    val driverRunState = driver.runAndWait(workingOozieTransformation)

    driverRunState shouldBe a[DriverRunSucceeded[OozieTransformation]]
  }

  it should "execute oozie tranformations asynchronously" taggedAs (DriverTests, OozieTests) in {
    val driverRunHandle = driver.run(workingOozieTransformation)

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[OozieTransformation]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunSucceeded[OozieTransformation]]
  }

  it should "execute oozie tranformations and return errors while running synchronously" taggedAs (DriverTests, OozieTests) in {
    val driverRunState = driver.runAndWait(failingOozieTransformation)

    driverRunState shouldBe a[DriverRunFailed[OozieTransformation]]
  }

  it should "execute oozie tranformations and return errors while running asynchronously" taggedAs (DriverTests, OozieTests) in {
    val driverRunHandle = driver.run(failingOozieTransformation)

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[OozieTransformation]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunFailed[OozieTransformation]]
  }

  it should "be able to kill running oozie transformations" taggedAs (DriverTests, OozieTests) in {
    val driverRunHandle = driver.run(workingOozieTransformation)
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunOngoing[OozieTransformation]]
    driver.killRun(driverRunHandle)

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[OozieTransformation]]) {
      Thread.sleep(1000)
    }

    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunFailed[OozieTransformation]]
  }
}