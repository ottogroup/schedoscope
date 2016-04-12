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
package org.schedoscope.dsl

import com.openpojo.reflection.impl.PojoClassFactory
import org.schedoscope.dsl.storageformats._
import org.schedoscope.dsl.transformations.{ NoOp, Transformation }
import org.schedoscope.dsl.views.ViewUrlParser
import org.schedoscope.dsl.views.ViewUrlParser.{ ParsedView, ParsedViewAugmentor }
import org.schedoscope.test.rows
import scala.Array.canBuildFrom
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.{ HashMap, HashSet, ListBuffer }
import scala.language.{ existentials, implicitConversions }
import org.schedoscope.dsl.transformations.SeqTransformation

/**
 * Base class for all view definitions. Provides all features of structures and view DSLs.
 */
abstract class View extends Structure with ViewDsl with DelayedInit {

  def lowerCasePackageName = Named.camelToLowerUnderscore(getClass.getPackage.getName)

  override def toString() = urlPath

  def nWithoutPartitioningSuffix = super.n

  override def n =
    if (!hasSuffixPartitions)
      nWithoutPartitioningSuffix
    else
      nWithoutPartitioningSuffix + "_" + suffixPartitionParameters.map { p => p.v.get }.mkString("_").toLowerCase()

  /**
   * The package and view class prefix of the URL syntax representing the present view
   */
  def urlPathPrefix = s"${lowerCasePackageName}/${namingBase.replaceAll("[^a-zA-Z0-9]", "")}"

  /**
   * The URL path syntax identifying the present view.
   */
  def urlPath = s"${urlPathPrefix}/${partitionValues(false).mkString("/")}"

  /**
   * The view's environment.
   */
  var env = "dev"

  /**
   * Pluggable builder function that returns the name of the module the view belongs to.
   * The default implemementation returns the view's package in database-friendly lower-case underscore format, replacing all . with _.
   */
  override var moduleNameBuilder = () => lowerCasePackageName.replaceAll("[.]", "_")

  def module = moduleNameBuilder()

  /**
   * Pluggable builder function that returns the database name for the view given an environment.
   * The default implementation prepends the environment to the result of moduleNameBuilder with an underscore.
   */
  override var dbNameBuilder = (env: String) => env.toLowerCase() + "_" + moduleNameBuilder()

  def dbName = dbNameBuilder(env)

  /**
   * Pluggable builder function that returns the table name for the view given an environment.
   * The default implementation appends the view's name n to the result of dbNameBuilder.
   */
  override var tableNameBuilder = (env: String) => dbNameBuilder(env) + "." + n

  def tableName = tableNameBuilder(env)

  /**
   * Pluggable builder function that returns the HDFS path representing the database of the view given an environment.
   * The default implementation does this by building a path from the lower-case-underscore format of
   * moduleNameBuilder, replacing _ with / and prepending hdp/dev/ for the default dev environment.
   */
  override var dbPathBuilder = (env: String) => ("_hdp_" + env.toLowerCase() + "_" + moduleNameBuilder().replaceFirst("app", "applications")).replaceAll("_", "/")

  def dbPath = dbPathBuilder(env)

  /**
   * Pluggable builder function that returns the HDFS path to the table the view belongs to.
   * The default implementation does this by joining dbPathBuilder and n. The latter will
   * be surrounded by additionalStoragePathPrefix and additionalStoragePathSuffix, if set.
   */
  override var tablePathBuilder = (env: String) => dbPathBuilder(env) + (if (additionalStoragePathPrefix.isDefined) "/" + additionalStoragePathPrefix.get else "") + "/" + n + (if (additionalStoragePathSuffix.isDefined) "/" + additionalStoragePathSuffix.get else "")

  def tablePath = tablePathBuilder(env)

  /**
   * Pluggable builder function that returns the relative partition path for the view. By default,
   * this is the standard Hive /partitionColumn=value/... pattern.
   */
  override var partitionPathBuilder = () => partitionSpec

  def partitionPath = partitionPathBuilder()

  /**
   * Pluggable builder function that returns the full HDFS path to the partition represented by the view.
   * The default implementation concatenates the output of tablePathBuilder and partitionPathBuilder for
   * this purpose.
   */
  override var fullPathBuilder = (env: String) => tablePathBuilder(env) + partitionPathBuilder()

  def fullPath = fullPathBuilder(env)

  /**
   * Pluggable builder function returning a path prefix of where Avro schemas can be found in HDFS.
   * By default, this is hdfs:///hdp/$\{env\}/global/datadictionary/schema/avro
   */
  override var avroSchemaPathPrefixBuilder = (env: String) => s"hdfs:///hdp/${env}/global/datadictionary/schema/avro"

  def avroSchemaPathPrefix = avroSchemaPathPrefixBuilder(env)

  /**
   * Returns true if the present view is partitionend.
   */
  def isPartitioned() = partitionParameters.nonEmpty

  /**
   * Returns true if the passed parameter is a paritioning parameter of the view.
   */
  def isPartition(p: Parameter[_]) = parameters.contains(p)

  def registerParameter(p: Parameter[_]) {
    p.assignTo(this)
  }

  /**
   * Returns all parameters of the present view in ascending order of their weight.
   */
  def parameters = fieldLikeGetters
    .filter { m => classOf[Parameter[_]].isAssignableFrom(m.getReturnType()) }
    .map { m => m.invoke(this).asInstanceOf[Parameter[_]] }
    .filter { m => m != null }
    .sortWith {
      _.orderWeight < _.orderWeight
    }
    .toSeq

  /**
   * Returns all parameters that are not suffix parameters (i.e., real partitioning parameters) of the present view
   * in ascending order of their weight.
   */
  def partitionParameters = parameters
    .filter { p => isPartition(p) && !isSuffixPartition(p) }

  /**
   * Returns the Hive partition pattern (/partitionColumns=value/...) for the present view observing order weight.
   */
  def partitionSpec = "/" + partitionParameters.map(p => s"${p.n}=${p.v.getOrElse("")}").mkString("/")

  /**
   * Returns a list of partition values in order the parameter weights. Such lists are necessary for communicating with the metastore.
   */
  def partitionValues(ignoreSuffixPartitions: Boolean = true) =
    (if (ignoreSuffixPartitions)
      partitionParameters
    else
      parameters).map(p => p.v.getOrElse("").toString).toList

  private val suffixPartitions = new HashSet[Parameter[_]]()

  def asTableSuffix[P <: Parameter[_]](p: P): P = {
    suffixPartitions.add(p)
    p
  }

  /**
   * Checks wether a given parameter is implemented using a table name suffix.
   */
  def isSuffixPartition(p: Parameter[_]) = suffixPartitions.contains(p)

  /**
   * Are there any parameters implemented as table name suffixes?
   */
  def hasSuffixPartitions = !suffixPartitions.isEmpty

  def suffixPartitionParameters = parameters
    .filter { p => isPartition(p) && isSuffixPartition(p) }

  private val deferredDependencies = ListBuffer[() => Seq[View]]()

  /**
   * Return all dependencies of the view in the order they have been declared.
   */
  def dependencies = deferredDependencies.flatMap {
    _()
  }.distinct

  /**
   * Add dependencies to the given view. This is done with an anonymous function returning a sequence of views the
   * current view depends on.
   */
  def dependsOn[V <: View: Manifest](dsf: () => Seq[V]) {
    val df = () => dsf().map {
      View.register(this.env, _)
    }

    deferredDependencies += df
  }

  /**
   * Add a dependency to the given view. This is done with an anonymous function returning a view the
   * current view depends on. This function is returned so that it can be assigned to variables for further reference.
   */
  def dependsOn[V <: View: Manifest](df: () => V) = {
    val dsf = () => List(View.register(this.env, df()))

    dependsOn(dsf)

    () => dsf().head
  }

  /**
   * The rank of the view. Views without dependencies are of Rank 0, all others are one rank higher than the
   * of biggest rank of their dependencies
   */
  lazy val rank: Int = {
    val ds = dependencies
    if (ds.isEmpty)
      0
    else
      ds.map {
        _.rank
      }.max + 1
  }

  var storageFormat: StorageFormat = TextFile()
  var additionalStoragePathPrefix: Option[String] = None
  var additionalStoragePathSuffix: Option[String] = None

  /**
   * Specifiy the storage format of the view, with TextFile being the default. One can optionally specify storage path prefixes and suffixes.
   */
  def storedAs(f: StorageFormat, additionalStoragePathPrefix: String = null, additionalStoragePathSuffix: String = null) {
    storageFormat = f
    this.additionalStoragePathPrefix = if (additionalStoragePathPrefix != null) Some(additionalStoragePathPrefix) else None
    this.additionalStoragePathSuffix = if (additionalStoragePathSuffix != null) Some(additionalStoragePathSuffix) else None
  }

  var registeredTransformation: () => Transformation = () => NoOp()

  /**
   * Set the transformation with which the view is created. Provide an anonymous function returning the transformation.
   * NoOp is the default transformation if none is specified.
   */
  def transformVia(ft: () => Transformation) {
    ensureRegisteredParameters

    registeredTransformation = ft
  }

  def configureTransformation(k: String, v: Any) {
    ensureRegisteredParameters
    
    val t = registeredTransformation()
    transformVia(() => t.configureWith(Map((k, v))))
  }

  var registeredExports: List[() => Transformation] = List()

  /**
   * Registers an export transformation with the view. You need to provide an anonymous constructor function returning this transformation.
   * This transformation is executed after the "real" transformation registered with transformVia() has executed successfully.
   */
  def exportTo(export: () => Transformation) {
    ensureRegisteredParameters

    registeredExports ::= export
  }

  /**
   * This method returns the final transformation constructor function, usually the one registered by transformVia().
   * Registered exportTo() transformations will modify the resulting transformation.
   */
  def transformation = {
    ensureRegisteredParameters

    registeredExports
      .foldRight(registeredTransformation) {
        (export, transformationSoFar) => () => SeqTransformation(transformationSoFar(), export())
      }
  }

  var isMaterializeOnce = false

  def materializeOnce {
    isMaterializeOnce = true
  }

  /**
   * Dumbly registed all parameters with the view.
   */
  def ensureRegisteredParameters {
    for (p <- parameters)
      registerParameter(p)
  }

  /**
   * Dumbly registed all parameters with the view after the constructor is done.
   */
  def delayedInit(body: => Unit) {
    ensureRegisteredParameters
    body
  }
}

/**
 * View helpers. Also a registry of created views ensuring that there are no duplicate objects representing the same view.
 */
object View {
  private val knownViews = HashMap[View, View]()

  private def register[V <: View: Manifest](env: String, v: V): V = this.synchronized {
    val registeredView = knownViews.get(v) match {
      case Some(registeredView) => {
        registeredView.asInstanceOf[V]
      }
      case None => {
        knownViews.put(v, v)
        v
      }
    }
    registeredView.env = env
    registeredView
  }

  /**
   * Return all views from a given package.
   */
  def viewsInPackage(packageName: String): Seq[Class[View]] = {
    PojoClassFactory.getPojoClassesRecursively(packageName, null).filter {
      _.extendz(classOf[View])
    }.filter {
      !_.extendz(classOf[rows])
    }.filter {
      !_.isAbstract()
    }.map {
      _.getClazz()
    }.toSeq.asInstanceOf[Seq[Class[View]]]
  }

  /**
   * Return the traits implemented by a view.
   */
  def getTraits[V <: View: Manifest](viewClass: Class[V]) = {
    viewClass.getInterfaces().filter(_ != classOf[Serializable]).filter(_ != classOf[scala.Product])
  }

  /**
   * Instantiate a new view given its class name, an environment, and a list of parameter values.
   */
  def newView[V <: View: Manifest](viewClass: Class[V], env: String, parameterValues: TypedAny*): V = {
    val viewCompanionObjectClass = Class.forName(viewClass.getName() + "$")
    val viewCompanionConstructor = viewCompanionObjectClass.getDeclaredConstructor()
    viewCompanionConstructor.setAccessible(true)
    val viewCompanionObject = viewCompanionConstructor.newInstance()

    val applyMethods = viewCompanionObjectClass.getDeclaredMethods()
      .filter {
        _.getName() == "apply"
      }

    val viewConstructor = applyMethods
      .filter { apply =>
        val parameterTypes = apply.getGenericParameterTypes().distinct
        !((parameterTypes.length == 1) && (parameterTypes.head == classOf[Object]))
      }
      .head

    val parametersToPass = ListBuffer[Any]()
    val parameterValuesPassed = ListBuffer[TypedAny]()
    parameterValuesPassed ++= parameterValues

    if (viewConstructor.getParameterTypes.size > parameterValues.size) {
      throw new RuntimeException(s"Not enough arguments for constructing view ${viewClass.getSimpleName}; required ${viewConstructor.getParameterTypes.size}, found ${parameterValues.size}")
    }

    for (constructorParameterType <- viewConstructor.getParameterTypes()) {
      var passedValueForParameter: TypedAny = null

      for (parameterValue <- parameterValuesPassed; if passedValueForParameter == null) {
        if (constructorParameterType.isAssignableFrom(parameterValue.t.runtimeClass)) {
          passedValueForParameter = parameterValue
        }
      }

      if (passedValueForParameter != null) {
        parameterValuesPassed -= passedValueForParameter
      }

      parametersToPass += passedValueForParameter.v
    }

    register(env, viewConstructor.invoke(viewCompanionObject, parametersToPass.asInstanceOf[Seq[Object]]: _*).asInstanceOf[V])
  }

  /**
   * Instantiate views given an environment and view URL path. A parsed view augmentor can further modify the created views.
   */
  def viewsFromUrl(env: String, viewUrlPath: String, parsedViewAugmentor: ParsedViewAugmentor = new ParsedViewAugmentor() {}): List[View] =
    try {
      ViewUrlParser
        .parse(env, viewUrlPath)
        .map {
          parsedViewAugmentor.augment(_)
        }
        .filter {
          _ != null
        }
        .map { case ParsedView(env, viewClass, parameters) => newView(viewClass, env, parameters: _*) }
    } catch {
      case t: Throwable => throw new RuntimeException(s"Error while parsing view(s) ${viewUrlPath} : ${t.getMessage}")
    }

}
