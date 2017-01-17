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

import java.io.File

import org.schedoscope.scheduler.service.ViewTransformationStatus
import org.schedoscope.dsl.transformations.Transformation.replaceParameters
import org.schedoscope.scheduler.driver.FilesystemDriver

/**
  * This captures view transformation logic implemented as Spark jobs. It is expected to be available in a submittable
  * JAR or PY file.
  *
  * Spark transformations use their configuration to set Spark configuration values, Spark arguments, as well as environment
  * variables for the Spark job. This works as follows:
  *
  * (a) any configuration key starting with "spark." is considered a Spark configuration value.
  * (b) any configuration key starting with "--" is considered a Spark argument. The configuration value is passed as the value of the argument
  * (c) any other configuration key is considered an environment variable to be passed to the Spark job.
  *
  * @param applicationName An optional logical name for the Spark job
  * @param mainJarOrPy     Path to the JAR or Python file containing the Spark job
  * @param mainClass       In case of a JAR, the main class within that JAR
  * @param applicationArgs Command line arguments to pass. Defaults to an empty list
  * @param master          Spark master setting. Defaults to "yarn-cluster".
  * @param deployMode      Spark deployment mode setting. Defaults to "cluster"
  * @param additionalJars  A list of optional JAR files to deploy with the job. Defaults to an empty list.
  * @param additionalPys   A list of optional Python files to deploy with the job. Defaults to an empty list.
  * @param additionalFiles A list of optional files to deploy with the job. Defaults to an empty list.
  * @param propertiesFile  Path to an optional properties file.
  */
case class SparkTransformation(
                                applicationName: String = "", mainJarOrPy: String, mainClass: String = null,
                                applicationArgs: List[String] = List(),
                                master: String = "yarn-cluster", deployMode: String = "cluster",
                                additionalJars: List[String] = List(),
                                additionalPys: List[String] = List(),
                                additionalFiles: List[String] = List(),
                                propertiesFile: String = null
                              ) extends Transformation {
  def name = "spark"

  override def stringsToChecksum = if (mainClass == null) List() else List(mainClass)

  override def fileResourcesToChecksum = List(mainJarOrPy)

  override def viewTransformationStatus = ViewTransformationStatus(
    name,
    Some(Map(
      "applicationName" -> applicationName,
      "mainJarOrPy" -> mainJarOrPy,
      "mainClass" -> {
        if (mainClass == null) "" else mainClass
      },
      "applicationArgs" -> applicationArgs.mkString(", "),
      "master" -> master,
      "deployMode" -> deployMode,
      "additionalJars" -> additionalJars.mkString(", "),
      "additionalPys" -> additionalPys.mkString(", "),
      "additionalFiles" -> additionalFiles.mkString(", "),
      "propertiesFile" -> {
        if (propertiesFile == null) "" else propertiesFile
      }
    )))
}

object SparkTransformation {
  /**
    * Helper to return the path of a Spark job object on the Schedoscope classpath as a means to fill in the mainJarOrPy
    * parameter of a Spark transformation
    *
    * @param o the Spark job object
    * @return the path to the JAR file with the object
    */
  def jarOf(o: AnyRef) = o.getClass.getProtectionDomain().getCodeSource().getLocation().getFile

  /**
    * Helper to return the class name of a Spark job object on the Schedoscope classpath as a means to fill in the mainJarOrPy
    * parameter of a Spark transformation
    *
    * @param o the Spark job object
    * @return the class name
    */
  def classNameOf(o: AnyRef) = o.getClass.getName.replaceAll("\\$$", "")


  /**
    * Return an absolute path for a resource on the classpath so that it can be passed as the mainJarOrPy of a Spark transformation.
    * @param resourcePath The path to the resource. Needs to start with "classpath://"
    * @return the absolute path to the resource.
    */
  def resource(resourcePath: String) = new File(FilesystemDriver.classpathResourceToFile(resourcePath)).getPath.toString

  /**
    * Constructs a Spark transformation of out of a Hive transformation by taking its query and passing it to SparkSQLRunner.
    * @param t  the Hive transformation to run on Spark
    * @param master          Spark master setting. Defaults to "yarn-cluster".
    * @param deployMode      Spark deployment mode setting. Defaults to "cluster"
    * @param additionalJars  A list of optional JAR files to deploy with the job. Defaults to an empty list.
    * @param additionalPys   A list of optional Python files to deploy with the job. Defaults to an empty list.
    * @param additionalFiles A list of optional files to deploy with the job. Defaults to an empty list.
    * @param propertiesFile  Path to an optional properties file.
    * @return the Spark transformation running the Hive query.
    */
  def runOnSpark(t: Transformation,
                 master: String = "yarn-cluster", deployMode: String = "cluster",
                 additionalJars: List[String] = List(),
                 additionalPys: List[String] = List(),
                 additionalFiles: List[String] = List(),
                 propertiesFile: String = null): SparkTransformation =

    SparkTransformation(
      classNameOf(SparkSQLRunner), jarOf(SparkSQLRunner), classNameOf(SparkSQLRunner),
      List({
        val h = t.asInstanceOf[HiveTransformation]
        replaceParameters(h.sql, h.configuration.toMap).stripMargin
      }),
      master, deployMode,
      additionalJars,
      additionalPys,
      additionalFiles,
      propertiesFile
    )
  
}