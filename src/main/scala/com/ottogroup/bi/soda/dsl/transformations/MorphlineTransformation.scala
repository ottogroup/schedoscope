package com.ottogroup.bi.soda.dsl.transformations

import com.ottogroup.bi.soda.dsl.Transformation
import org.kitesdk.morphline.stdlib.DropRecordBuilder
import org.kitesdk.morphline.api.Command
import com.ottogroup.bi.soda.dsl.Named
import com.ottogroup.bi.soda.dsl.FieldLike
import com.ottogroup.bi.soda.dsl.ExternalTransformation

case class MorphlineTransformation(definition: String="",
                                   imports: Seq[String] = List(),
                                   sampling: Int = 100,
                                   anonymize : Seq[Named] = List(),
                                   fields : Seq[Named] = List(),
                                   fieldMapping : Map[FieldLike[_],FieldLike[_]] = Map()
                                   ) extends ExternalTransformation {
  def name() = "morphline"

}
