package org.schedoscope.dsl.transformations

/**
 * A shell transformation - externally transform a view using a shell script.
 *
 * @param script contents of the script to be executed
 * @param scriptFile path to script to be executed
 * @param shell path to the interpreter executable, defaults to /bin/bash
 *
 * Either script or scriptFile needs to be specified. Environment variables can be set using the transformation's configuration.
 *
 */
case class ShellTransformation(script: String = "", scriptFile: String = "", shell: String = "/bin/bash") extends Transformation {
  def name = "shell"

  override def fileResourcesToChecksum = if (scriptFile != "") List(scriptFile) else List()

  override def stringsToChecksum = List(script)
}
