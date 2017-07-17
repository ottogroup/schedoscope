package org.schedoscope.dsl.transformations

import org.schedoscope.{Schedoscope, Settings}
import org.schedoscope.dsl.View

object SshDistcpTransformation {

  def copyFromProd(source: String, targetView: View, machine: String, mapper: Int = 50): ShellTransformation = {
    val target = targetView.fullPath.split("/").dropRight(1).mkString("/")
    val namenode = Schedoscope.settings.nameNode

    ShellTransformation(s"ssh -K $machine 'hadoop -distcp -m $mapper $source hdfs://$namenode$target'")
  }

}



