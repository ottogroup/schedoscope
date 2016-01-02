package org.schedoscope.dsl.transformations

/**
 * A shell transformation - externally transform a view using a shell script.
 *
 * @param script contents of the script to be executed
 * @param scriptFile path to script to be executed
 *
 *                   Either script or scriptFile needs to be specified
 *
 * @param shell path to the interpreter executable, defaults to /bin/bash
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
