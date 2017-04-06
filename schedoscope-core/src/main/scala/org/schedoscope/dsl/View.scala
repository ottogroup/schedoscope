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
import org.schedoscope.Schedoscope
import org.schedoscope.dsl.storageformats._
import org.schedoscope.dsl.transformations.{NoOp, SeqTransformation, Transformation}
import org.schedoscope.dsl.views.ViewUrlParser
import org.schedoscope.dsl.views.ViewUrlParser.{ParsedView, ParsedViewAugmentor}
import org.schedoscope.lineage.{DependencyAnalyzer, DependencyMap}
import org.schedoscope.test.WritableView
import org.slf4j.LoggerFactory

import scala.Array.canBuildFrom
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable
import scala.collection.mutable.{HashMap, HashSet, ListBuffer}
import scala.language.{existentials, implicitConversions}
import scala.util.{Failure, Success, Try}

/**
  * Base class for all view definitions. Provides all features of structures and view DSLs.
  */
abstract class View extends Structure with ViewDsl with DelayedInit {

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

  val isExternal = false
  val suffixPartitions = new HashSet[Parameter[_]]()
  private val deferredDependencies = ListBuffer[() => Seq[View]]()

  /**
    * Pluggable builder function that returns the name of the module the view belongs to.
    * The default implemementation returns the view's package in database-friendly lower-case underscore format, replacing all . with _.
    */
  override var moduleNameBuilder = () => lowerCasePackageName.replaceAll("[.]", "_")

  /**
    * Pluggable builder function that returns the database name for the view given an environment.
    * The default implementation prepends the environment to the result of moduleNameBuilder with an underscore.
    */
  override var dbNameBuilder = (env: String) => env.toLowerCase() + "_" + moduleNameBuilder()

  /**
    * Pluggable builder function that returns the table name for the view given an environment.
    * The default implementation appends the view's name n to the result of dbNameBuilder.
    */
  override var tableNameBuilder = (env: String) => dbNameBuilder(env) + "." + n

  /**
    * Pluggable builder function that returns either the HDFS or AWS S3 path representing
    * the database of the view given an environment
    * The default implementation does this by building a path from the lower-case-underscore format of
    * moduleNameBuilder, replacing _ with / and prepending /hdp/dev/ for the default dev environment.
    */
  override var dbPathBuilder = (env: String) =>
    (if (s3Bucket.isDefined && s3UriScheme.isDefined)
      s3BucketPathBuilder(s3Bucket.get, s3UriScheme.get)
    else
      Schedoscope.settings.viewDataHdfsRoot) + "/" + env.toLowerCase() +
      "/" + moduleNameBuilder()
      .replaceFirst("app", "applications").replaceAll("_", "/")

  /**
    * Pluggable builder function that returns the HDFS path to the table the view belongs to.
    * The default implementation does this by joining dbPathBuilder and n. The latter will
    * be surrounded by additionalStoragePathPrefix and additionalStoragePathSuffix, if set.
    */
  override var tablePathBuilder = (env: String) => dbPathBuilder(env) +
    (if (additionalStoragePathPrefix.isDefined) "/" + additionalStoragePathPrefix.get else "") +
    "/" +
    n +
    (if (additionalStoragePathSuffix.isDefined) "/" + additionalStoragePathSuffix.get else "")
  /**
    * Pluggable builder function that returns the relative partition path for the view. By default,
    * this is the standard Hive /partitionColumn=value/... pattern.
    */
  override var partitionPathBuilder = () => partitionSpec
  /**
    * Pluggable builder function that returns the full HDFS path to the partition represented by the view.
    * The default implementation concatenates the output of tablePathBuilder and partitionPathBuilder for
    * this purpose.
    */
  override var fullPathBuilder = (env: String) => tablePathBuilder(env) + partitionPathBuilder()
  /**
    * Pluggable builder function returning a path prefix of where Avro schemas can be found in HDFS.
    * By default, this is hdfs:///hdp/$\{env\}/global/datadictionary/schema/avro
    */
  override var avroSchemaPathPrefixBuilder = (env: String) => s"hdfs:///hdp/${env}/global/datadictionary/schema/avro"

  /**
    * The view's environment.
    */
  private var _env: String = "dev"

  def env: String = _env

  def env_=(env: String): Unit = {
    _env = env
  }

  var storageFormat: StorageFormat = TextFile()
  var additionalStoragePathPrefix: Option[String] = None
  var additionalStoragePathSuffix: Option[String] = None
  var registeredTransformation: () => Transformation = () => NoOp()
  var registeredExports: List[() => Transformation] = List()
  var isMaterializeOnce = false

  var tblProperties = HashMap[String, String]()

  /**
    * Optional use of S3 to store hive data
    */
  var s3Bucket: Option[String] = None
  var s3UriScheme: Option[String] = None

  var s3BucketPathBuilder = (bucketName: String, uriScheme: String) => s"${uriScheme}://${bucketName}"

  override def toString() = urlPath

  /**
    * Proposed name for the transformation materializing this view
    *
    * @return
    */
  def shortUrlPath = {
    val dbPath = dbName
      .split("_").takeRight(2)
      .map(_.substring(0, 1))
      .mkString(".")
    s"${dbPath}/${namingBase.replaceAll("[^a-zA-Z0-9]", "")}/${partitionValues(false).mkString("/")}"
  }

  /**
    * The URL path syntax identifying the present view.
    */
  def urlPath = s"${urlPathPrefix}/${partitionValues(false).mkString("/")}"

  /**
    * The package and view class prefix of the URL syntax representing the present view
    */
  def urlPathPrefix = s"${lowerCasePackageName}/${namingBase.replaceAll("[^a-zA-Z0-9]", "")}"

  def lowerCasePackageName = Named.camelToLowerUnderscore(getClass.getPackage.getName)

  /**
    * Returns a list of partition values in order the parameter weights. Such lists are necessary for communicating with the metastore.
    */
  def partitionValues(ignoreSuffixPartitions: Boolean = true) =
    (if (ignoreSuffixPartitions)
      partitionParameters
    else
      parameters).map(p => p.v.getOrElse("").toString).toList

  /**
    * Returns all parameters that are not suffix parameters (i.e., real partitioning parameters) of the present view
    * in ascending order of their weight.
    */
  def partitionParameters = parameters
    .filter { p => isPartition(p) && !isSuffixPartition(p) }

  override def n =
    if (!hasSuffixPartitions)
      nWithoutPartitioningSuffix
    else
      nWithoutPartitioningSuffix + "_" + suffixPartitionParameters.map { p => p.v.get }.mkString("_").toLowerCase()

  def nWithoutPartitioningSuffix = super.n

  /**
    * Are there any parameters implemented as table name suffixes?
    */
  def hasSuffixPartitions = !suffixPartitions.isEmpty

  def suffixPartitionParameters = parameters
    .filter { p => isPartition(p) && isSuffixPartition(p) }

  /**
    * Returns true if the passed parameter is a paritioning parameter of the view.
    */
  def isPartition(p: Parameter[_]) = parameters.contains(p)

  /**
    * Checks whether a given parameter is implemented using a table name suffix.
    */
  def isSuffixPartition(p: Parameter[_]) = suffixPartitions.contains(p)

  def module = moduleNameBuilder()

  def dbName = dbNameBuilder(env)

  def tableName = tableNameBuilder(env)

  def dbPath = dbPathBuilder(env)

  def tablePath = tablePathBuilder(env)

  def partitionPath = partitionPathBuilder()

  def fullPath = fullPathBuilder(env)

  def avroSchemaPathPrefix = avroSchemaPathPrefixBuilder(env)

  /**
    * Return all dependencies of this view recursively
    */
  lazy val recursiveDependencies: Set[View] = View.recursiveDependenciesOf(this).toSet

  /**
    * Returns true if the present view is partitionend.
    */
  def isPartitioned() = partitionParameters.nonEmpty

  /**
    * Returns the Hive partition pattern (/partitionColumns=value/...) for the present view observing order weight.
    */
  def partitionSpec = "/" + partitionParameters.map(p => s"${p.n}=${p.v.getOrElse("")}").mkString("/")

  override def asTableSuffix[P <: Parameter[_]](p: P): P = {
    suffixPartitions.add(p)
    p
  }

  /**
    * Add a dependency to the given view. This is done with an anonymous function returning a view the
    * current view depends on. This function is returned so that it can be assigned to variables for further reference.
    */
  override def dependsOn[V <: View : Manifest](df: () => V) = {
    val dsf = () => List(View.register(this.env, df()))

    dependsOn(dsf)

    () => dsf().head
  }

  /**
    * Add dependencies to the given view. This is done with an anonymous function returning a sequence of views the
    * current view depends on.
    */
  override def dependsOn[V <: View : Manifest](dsf: () => Seq[V]) {
    val df = () => dsf().map {
      View.register(this.env, _)
    }

    deferredDependencies += df
  }

  /**
    * Specify the storage format of the view, with TextFile being the default. One can optionally specify storage path prefixes and suffixes.
    */
  override def storedAs(f: StorageFormat, additionalStoragePathPrefix: String = null, additionalStoragePathSuffix: String = null) {
    storageFormat = f
    this.additionalStoragePathPrefix = Option(additionalStoragePathPrefix)
    this.additionalStoragePathSuffix = Option(additionalStoragePathSuffix)

    f match {
      case S3(bucketName, storageFormat, uriScheme) =>
        s3Bucket = Some(bucketName)
        s3UriScheme = Some(uriScheme)
        storedAs(storageFormat, additionalStoragePathPrefix, additionalStoragePathSuffix)

      case _ =>
      // do nothing ..

    }

  }

  /**
    * Specify table properties of a view, which is implemented in Hive as clause TBLPROPERTIES
    */
  override def tblProperties(m: Map[String, String]): Unit = tblProperties ++= m

  /**
    * Postfactum configuration of the registered transformation. Useful to override transformation configs within a test.
    */
  def configureTransformation(k: String, v: Any) {
    ensureRegisteredParameters

    val t = registeredTransformation()
    transformVia(() => t.configureWith(Map(k -> v)))
  }

  /**
    * Set the transformation with which the view is created. Provide an anonymous function returning the transformation.
    * NoOp is the default transformation if none is specified.
    */
  override def transformVia(ft: () => Transformation) {
    ensureRegisteredParameters

    registeredTransformation = ft
  }

  /**
    * Registers an export transformation with the view. You need to provide an anonymous constructor function returning this transformation.
    * This transformation is executed after the "real" transformation registered with transformVia() has executed successfully.
    */
  override def exportTo(export: () => Transformation) {
    ensureRegisteredParameters

    registeredExports ::= export
  }


  /**
    * Remove all exports, for example in tests
    */
  def muteExports() {
    registeredExports = List()
  }

  /**
    * Postfactum configuration of the registered exports. Useful to override export configs within a test.
    */
  def configureExport(k: String, v: Any) {
    ensureRegisteredParameters

    val reconfiguredExports = registeredExports.map { e => () => e().configureWith(Map(k -> v)) }

    registeredExports = reconfiguredExports
  }

  /**
    * Dumbly registed all parameters with the view.
    */
  def ensureRegisteredParameters {
    for (p <- parameters)
      registerParameter(p)
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

  def registerParameter(p: Parameter[_]) {
    p.assignTo(this)
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

  /**
    * Materialize once makes sure that the given view is only materialized once, even if its dependencies or version checksum change
    * afterwards.
    */
  override def materializeOnce {
    isMaterializeOnce = true
  }

  /**
    * Dumbly registed all parameters with the view after the constructor is done.
    */
  def delayedInit(body: => Unit) {
    ensureRegisteredParameters
    body
  }

  def fieldsAndParameters: Seq[FieldLike[_]] = Seq[FieldLike[_]]() ++ fields ++ partitionParameters

  var explicitLineage: Map[FieldLike[_], mutable.Set[FieldLike[_]]] = Map()

  def affects(influenceFunc: this.type => Traversable[(FieldLike[_], FieldLike[_])]): this.type = {
    influenceFunc(this).foreach {
      case (influencer, influencee) =>
        val ownerView: View = influencee.assignedStructure.get.asInstanceOf[View]
        if (ownerView.explicitLineage.isEmpty)
          ownerView.explicitLineage = ownerView.fields.map(f => f -> mutable.Set[FieldLike[_]]()).toMap

        ownerView.explicitLineage(influencee).add(influencer)
    }

    this
  }

  def lineage: DependencyMap = tryLineage match {
    case Success(d) => d
    case Failure(ex) =>
      LoggerFactory.getLogger(getClass).warn("Cannot analyze lineage, falling back to black-box: ", ex)
      DependencyAnalyzer.getBlackboxLineage(this)
  }

  def tryLineage: Try[DependencyMap] = DependencyAnalyzer.analyzeLineage(this)

  /**
    * Return all dependencies of the view in the order they have been declared.
    */
  def dependencies: List[View] = deferredDependencies.flatMap {
    _ ()
  }.distinct.toList

  /**
    * Returns all transitive dependencies of this view.
    */
  def transitiveDependencies: Set[View] = View.recursiveDependenciesOf(this).toSet - this

  /**
    * Returns true if views contains external dependencies
    */
  def hasExternalDependencies = dependencies.exists(_.isExternal)

  def isInDatabases(databases: String*): Boolean = {
    val name = dbName.replace("_", ".")

    databases.exists {
      s =>
        name.startsWith(s.replace("${env}", env))
    }
  }
}

/**
  * View helpers. Also a registry of created views ensuring that there are no duplicate objects representing the same view.
  */
object View {
  private val knownViews = HashMap[String, View]()

  /**
    * Return all views from a given package.
    */
  def viewsInPackage(packageName: String): Seq[Class[View]] = {
    PojoClassFactory.getPojoClassesRecursively(packageName, null).filter {
      _.extendz(classOf[View])
    }.filter {
      !_.extendz(classOf[WritableView])
    }.filter {
      !_.isAbstract()
    }.map {
      _.getClazz()
    }.toSeq.asInstanceOf[Seq[Class[View]]]
  }


  /**
    * Return the traits implemented by a view.
    */
  def getTraits[V <: View : Manifest](viewClass: Class[V]) = {
    viewClass.getInterfaces().filter(_ != classOf[Serializable]).filter(_ != classOf[scala.Product])
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
      case t: Throwable =>
        if (t.isInstanceOf[java.lang.reflect.InvocationTargetException]) {
          throw new RuntimeException(s"Error while parsing view(s) ${viewUrlPath} : ${t.getCause().getMessage}")
        } else {
          throw new RuntimeException(s"Error while parsing view(s) ${viewUrlPath} : ${t.getMessage}")
        }
    }

  /**
    * Instantiate a new view given its class name, an environment, and a list of parameter values.
    */
  def newView[V <: View : Manifest](viewClass: Class[V], env: String, parameterValues: TypedAny*): V = {
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

  private def register[V <: View : Manifest](env: String, v: V): V = this.synchronized {
    val registeredView = knownViews.get(v.urlPath) match {
      case Some(registeredView) => {
        registeredView.asInstanceOf[V]
      }
      case None => {
        knownViews.put(v.urlPath, v)
        v
      }
    }
    registeredView.env = env
    registeredView
  }

  private def recursiveDependenciesOf(view: View, soFar: mutable.Set[View] = mutable.Set[View]()): mutable.Set[View] = {
    if (soFar.add(view))
      view.dependencies.foreach(recursiveDependenciesOf(_, soFar))
    soFar
  }
}


