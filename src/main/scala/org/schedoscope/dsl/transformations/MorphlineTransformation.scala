package org.schedoscope.dsl.transformations

import org.schedoscope.dsl._
import org.kitesdk.morphline.stdlib.DropRecordBuilder
import org.kitesdk.morphline.api.Command
import org.schedoscope.dsl.Named
import org.schedoscope.dsl.FieldLike
import org.schedoscope.dsl.ExternalTransformation

case class MorphlineTransformation(definition: String = "",
  imports: Seq[String] = List(),
  sampling: Int = 100,
  anonymize: Seq[Named] = List(),
  fields: Seq[Named] = List(),
  fieldMapping: Map[FieldLike[_], FieldLike[_]] = Map()) extends ExternalTransformation {
  def name() = "morphline"

  override def versionDigest = Version.digest(resourceHashes :+ definition)

  override def resources() = {
    List()
  }

}
