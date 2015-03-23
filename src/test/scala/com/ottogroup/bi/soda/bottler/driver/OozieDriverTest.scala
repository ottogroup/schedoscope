package com.ottogroup.bi.soda.bottler.driver

import org.apache.hadoop.fs.Path
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.ottogroup.bi.soda.DriverTests
import com.ottogroup.bi.soda.OozieTests
import com.ottogroup.bi.soda.dsl.transformations.OozieTransformation
import com.ottogroup.bi.soda.test.resources.OozieTestResources

class OozieDriverTest extends FlatSpec with Matchers {

  lazy val resources: OozieTestResources = OozieTestResources()

  def cluster = resources.mo
  def driver = resources.oozieDriver

  def deployWorkflow(wf: OozieTransformation) = {
    val hdfs = resources.fileSystem
    val dest = new Path(s"${resources.namenode}/${wf.workflowAppPath}")
    val src = new Path(s"src/test/resources/oozie/${wf.bundle}/${wf.workflow}/workflow.xml")

    println(s"Uploading workflow ${wf.workflow} from ${src} to ${dest}")

    if (!hdfs.exists(dest))
      hdfs.mkdirs(dest)

    hdfs.copyFromLocalFile(src, dest)
    wf
  }

  lazy val workingOozieTransformation = deployWorkflow(
    OozieTransformation(
      "bundle", "workflow",
      "/tmp/soda/oozie/workflows/bundle/workflow/", Map(
        "jobTracker" -> cluster.getJobTrackerUri(),
        "nameNode" -> cluster.getNameNodeUri(),
        "oozie.use.system.libpath" -> "false")))

  lazy val failingOozieTransformation = deployWorkflow(
    OozieTransformation(
      "bundle", "failflow",
      "/tmp/soda/oozie/workflows/bundle/failflow/", Map(
        "jobTracker" -> cluster.getJobTrackerUri(),
        "nameNode" -> cluster.getNameNodeUri(),
        "oozie.use.system.libpath" -> "false")))

  "Oozie" should "have transformation name oozie" taggedAs (DriverTests, OozieTests) in {
    driver.transformationName shouldBe "oozie"
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