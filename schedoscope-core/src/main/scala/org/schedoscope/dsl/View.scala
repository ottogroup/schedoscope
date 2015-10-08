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

import scala.Array.canBuildFrom
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.ListBuffer
import com.openpojo.reflection.impl.PojoClassFactory
import org.schedoscope.Settings
import org.schedoscope.dsl.View._
import org.schedoscope.dsl.views.ViewUrlParser
import org.schedoscope.dsl.views.ViewUrlParser.ParsedView
import org.schedoscope.dsl.views.ViewUrlParser.ParsedViewAugmentor
import org.schedoscope.test.rows

abstract class View extends Structure with ViewDsl with DelayedInit {

  var env = "dev"

  override def n = super.n + {
    val partitionings = parameters
      .filter { p => isPartition(p) && isSuffixPartition(p) }
      .map { p => p.v.get }

    if (partitionings.isEmpty)
      ""
    else
      "_" + partitionings.mkString("_").toLowerCase()
  }

  def nWithoutPartitioningSuffix = super.n

  var moduleNameBuilder: () => String = () => this.getClass().getPackage().getName()
  def module = Named.formatName(moduleNameBuilder()).replaceAll("[.]", "_")

  def urlPathPrefix = s"${Named.formatName(moduleNameBuilder())}/${namingBase.replaceAll("[^a-zA-Z0-9]", "")}"
  def urlPath = s"${urlPathPrefix}/${partitionValues.mkString("/")}"

  override def toString() = urlPath

  var dbNameBuilder: String => String = (env: String) => env.toLowerCase() + "_" + module
  def dbName = dbNameBuilder(env)
  def tableName = dbName + "." + n

  var moduleLocationPathBuilder: String => String = (env: String) => ("_hdp_" + env.toLowerCase() + "_" + module.replaceFirst("app", "applications")).replaceAll("_", "/")

  var locationPathBuilder: String => String = (env: String) => moduleLocationPathBuilder(env) + (if (additionalStoragePathPrefix.isDefined) "/" + additionalStoragePathPrefix.get else "") + "/" + n + (if (additionalStoragePathSuffix.isDefined) "/" + additionalStoragePathSuffix.get else "")
  def locationPath = locationPathBuilder(env)

  var partitionPathBuilder: () => String = () => partitionSpec
  def fullPath = locationPath + partitionPathBuilder()

  var avroSchemaPathPrefixBuilder: String => String = (env: String) => s"hdfs:///hdp/${env}/global/datadictionary/schema/avro"
  def avroSchemaPathPrefix = avroSchemaPathPrefixBuilder(env)

  private val suffixPartitions = new HashSet[Parameter[_]]()

  def asTableSuffix[P <: Parameter[_]](p: P): P = {
    suffixPartitions.add(p)
    p
  }

  def isPartition(p: Parameter[_]) = parameters.contains(p)

  def isSuffixPartition(p: Parameter[_]) = suffixPartitions.contains(p)

  def isPartitioned() = !partitionParameters.isEmpty()

  def registerParameter(p: Parameter[_]) {
    p.assignTo(this)
  }

  def parameters = fieldLikeGetters
    .filter { m => classOf[Parameter[_]].isAssignableFrom(m.getReturnType()) }
    .map { m => m.invoke(this).asInstanceOf[Parameter[_]] }
    .filter { m => m != null }
    .sortWith { _.orderWeight < _.orderWeight }
    .toSeq

  def partitionParameters = parameters
    .filter { p => isPartition(p) && !isSuffixPartition(p) }

  def partitionSpec = "/" + partitionParameters.map(p => s"${p.n}=${p.v.getOrElse("")}").mkString("/")
  def partitionValues = partitionParameters.map(p => p.v.getOrElse("").toString).toList

  private val deferredDependencies = ListBuffer[() => Seq[View]]()

  def dependencies = deferredDependencies.flatMap { _() }.distinct

  def dependsOn[V <: View: Manifest](dsf: () => Seq[V]) {
    val df = () => dsf().map { View.register(this.env, _) }

    deferredDependencies += df
  }

  def dependsOn[V <: View: Manifest](df: () => V) = {
    val dsf = () => List(View.register(this.env, df()))

    dependsOn(dsf)

    () => dsf().head
  }

  var storageFormat: StorageFormat = TextFile()
  var additionalStoragePathPrefix: Option[String] = None
  var additionalStoragePathSuffix: Option[String] = None

  def storedAs(f: StorageFormat, additionalStoragePathPrefix: String = null, additionalStoragePathSuffix: String = null) {
    storageFormat = f
    this.additionalStoragePathPrefix = if (additionalStoragePathPrefix != null) Some(additionalStoragePathPrefix) else None
    this.additionalStoragePathSuffix = if (additionalStoragePathSuffix != null) Some(additionalStoragePathSuffix) else None
  }

  var comment: Option[String] = None

  def comment(aComment: String) {
    comment = Some(aComment)
  }

  var transformation: () => Transformation = () => NoOp()

  def transformVia(ft: () => Transformation) {
    for (p <- parameters)
      registerParameter(p)

    transformation = ft
  }

  def configureTransformation(k: String, v: Any) {
    val t = transformation()
    transformVia(() => t.configureWith(Map((k, v))))
  }

  def isExternal = transformation().isInstanceOf[ExternalTransformation]

  var isMaterializeOnce = false

  def materializeOnce {
    isMaterializeOnce = true
  }
  
  def delayedInit(body: => Unit) {
    body

    for (p <- parameters)
      registerParameter(p)
  }
}

object View {
  private val knownViews = HashMap[View, View]()

  def register[V <: View: Manifest](env: String, v: V): V = this.synchronized {
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

  case class TypedAny(v: Any, t: Manifest[_])
  implicit def t[V: Manifest](v: V) = TypedAny(v, manifest[V])

  def viewsInPackage(packageName: String): Seq[Class[View]] = {
    PojoClassFactory.getPojoClassesRecursively(packageName, null).filter { _.extendz(classOf[View]) }.filter { !_.extendz(classOf[rows]) }.filter { !_.isAbstract() }.map { _.getClazz() }.toSeq.asInstanceOf[Seq[Class[View]]]
  }

  def getTraits[V <: View: Manifest](viewClass: Class[V]) = {
    viewClass.getInterfaces().filter(_ != classOf[Serializable]).filter(_ != classOf[scala.Product])
  }

  def newView[V <: View: Manifest](viewClass: Class[V], env: String, parameterValues: TypedAny*): V = {
    val viewCompanionObjectClass = Class.forName(viewClass.getName() + "$")
    val viewCompanionConstructor = viewCompanionObjectClass.getDeclaredConstructor()
    viewCompanionConstructor.setAccessible(true)
    val viewCompanionObject = viewCompanionConstructor.newInstance()

    val applyMethods = viewCompanionObjectClass.getDeclaredMethods()
      .filter { _.getName() == "apply" }

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
        if (constructorParameterType.isAssignableFrom(parameterValue.t.erasure)) {
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

  def viewsFromUrl(env: String, viewUrlPath: String, parsedViewAugmentor: ParsedViewAugmentor = new ParsedViewAugmentor() {}): List[View] =
    try {
      ViewUrlParser
        .parse(env, viewUrlPath)
        .map { parsedViewAugmentor.augment(_) }
        .filter { _ != null }
        .map { case ParsedView(env, viewClass, parameters) => newView(viewClass, env, parameters: _*) }
    } catch {
      case t: Throwable => throw new RuntimeException(s"Error while parsing view(s) ${viewUrlPath} : ${t.getMessage}")
    }

}
