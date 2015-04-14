package com.ottogroup.bi.soda.dsl.transformations

import com.ottogroup.bi.soda.dsl.Transformation
import org.kitesdk.morphline.stdlib.DropRecordBuilder
import org.kitesdk.morphline.api.Command

case class MorphlineTransformation(definition: String,
                                   imports: Seq[String] = List(),
                                   sampling: Int = 100,
                                   sink: String = "csv") extends Transformation {
  def name() = "morphline"

}
