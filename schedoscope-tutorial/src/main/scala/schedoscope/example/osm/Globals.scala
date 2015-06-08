package schedoscope.example.osm

import org.schedoscope.dsl.View
import java.text.SimpleDateFormat
import java.util.Date
import org.schedoscope.dsl.views.MonthlyParameterization
import org.schedoscope.Settings

object Globals {
  def defaultHiveQlParameters(v: View) = {
    val baseParameters = Map(
      "env" -> v.env,
      "workflow_time" -> new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").format(new Date),
      "workflow_name" -> v.getClass().getName())

    if (v.isInstanceOf[MonthlyParameterization])
      baseParameters ++ Map(
        "year" -> v.asInstanceOf[MonthlyParameterization].year.v.get,
        "month" -> v.asInstanceOf[MonthlyParameterization].month.v.get)
    else baseParameters
  }

  def defaultPigProperties(v:View) = Map(
    "exec.type" -> "MAPREDUCE",
    "mapred.job.tracker" -> Settings().jobTrackerOrResourceManager,
    "fs.default.name" -> Settings().nameNode,
    "env" -> v.env,
    "workflow_time" -> new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").format(new Date),
    "workflow_name" -> v.getClass().getName())
}