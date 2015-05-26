package com.ottogroup.bi.soda.bottler.driver

import com.ottogroup.bi.soda.dsl.Transformation
import com.ottogroup.bi.soda.dsl.transformations.oozie.OozieWF

class SodaDriver {

}

object SodaDriver {
  type ExecuteTransformation = (Transformation) => Boolean

  //  val execute:  ExecuteTransformation = (trans:Transformation) => trans match {
  //     case wf:OozieWF => OozieDriver.runAndWait(wf)
  //     case hiveQL:HiveQL => HiveDriver.runAndWait(hiveQL)
  //     case fileSystemOp:F
  //   }
}

