package org.schedoscope.dsl.transformations

import org.schedoscope.dsl.Transformation
import org.schedoscope.dsl.ExternalTransformation

/**
 * A shell transformation - can do anything
 * @param script contents of the script to be executed
 * @param scriptFile script to execute
 * Either script or scriptFile needs to be specified
 * @param shell path to the interpreter executable, could also be /bin/env python
 *
 */
case class ShellTransformation(script: String = "", scriptFile: String = "", shell: String = "/bin/bash", env: Map[String, String] = Map()) extends ExternalTransformation {
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
