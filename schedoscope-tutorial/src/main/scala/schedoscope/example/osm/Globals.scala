package schedoscope.example.osm

import org.schedoscope.dsl.View
import java.text.SimpleDateFormat
import java.util.Date
import org.schedoscope.dsl.views.MonthlyParameterization

object Globals {
    def defaultHiveQlParameters(v:View ) = Map(
    "env" -> v.env,
    "workflow_time" -> new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").format(new Date),
    "workflow_name" -> v.getClass().getName()
//    ,
//    "year" -> this.asInstanceOf[MonthlyParameterization].year.v.get,
//    "month" -> this.asInstanceOf[MonthlyParameterization].month.v.get
    )
}