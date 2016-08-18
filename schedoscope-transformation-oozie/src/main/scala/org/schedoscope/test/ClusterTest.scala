package org.schedoscope.test

import org.schedoscope.test.resources.OozieTestResources


/**
  * A test environment that is executed in a local minicluster
  */
trait clustertest extends test {
  resources = new OozieTestResources()

  def cluster = resources.asInstanceOf[OozieTestResources].mo
}
