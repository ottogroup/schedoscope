package org.schedoscope.dsl.transformations

import org.schedoscope.dsl.Transformation
import org.schedoscope.dsl.ExternalTransformation

case class ShellTransformation (script: String="",scriptFile:String="",shell:String="/bin/bash", env:Map[String,String]=Map()) extends  ExternalTransformation
{
  override def name = "shell"
}

object ShellTransformation {
  
    def environment(settings: Map[String, String] = Map()) = {
    val settingsStatements = new StringBuffer()

    for ((key, value) <- settings)
      settingsStatements.append(s"SET ${key}=${value};\n")

    settingsStatements.toString()
  }
  
}