package org.schedoscope.dsl.transformations

import collection.JavaConverters._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.tools.{DistCp, DistCpOptions}
import org.schedoscope.dsl.View


object MapreduceTransformation {

  def apply(v: View,
            sources: List[String],
            target: String,
            mappers: Int,
            overwrite: Boolean = false,
            update: Boolean = false,
            delete: Boolean = false,
            skipCrcCheck: Boolean = false): MapreduceTransformation = {

    val sourcePaths = sources.map(new Path(_)).asJava
    val options = new DistCpOptions(sourcePaths, new Path(target))
    options.setOverwrite(overwrite)
    options.setMaxMaps(mappers)
    options.setDeleteMissing(delete)
    options.setSkipCRC(skipCrcCheck)

    apply(v,options)
  }

  def apply(v: View,
            distCpOptions: DistCpOptions): MapreduceTransformation = {

  }

  def apply(v: View, source: String, target: String): MapreduceTransformation = {

    null
  }

}

case class DistCpTransformation(override val v: View,
                                override val createJob: Map[String, Any] => Job,
                                override val dirsToDelete: List[String] = List(),
                                override val deleteViewPath: Boolean = true)
  extends MapreduceTransformation(v, createJob) {


}
