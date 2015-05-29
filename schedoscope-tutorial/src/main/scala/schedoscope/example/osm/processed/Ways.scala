package schedoscope.example.osm.processed

import org.schedoscope.dsl.View
import org.schedoscope.dsl.views.Id
import org.schedoscope.dsl.views.PointOccurrence
import org.schedoscope.dsl.views.JobMetadata
import org.schedoscope.dsl.transformations.HiveTransformation
import org.schedoscope.dsl.transformations.HiveTransformation.insertInto
import org.schedoscope.dsl.transformations.HiveTransformation.queryFromResource
import org.schedoscope.dsl.Parquet
import java.util.Date
import java.text.SimpleDateFormat
import org.schedoscope.dsl.views.MonthlyParameterization
import org.schedoscope.Settings
import schedoscope.example.osm.Globals._
import org.schedoscope.dsl.Parameter
import org.schedoscope.dsl.views.DateParameterizationUtils._

case class Ways(
  year: Parameter[String] = today._1,
  month: Parameter[String] = today._2) extends View
    with Id
    with PointOccurrence
    with JobMetadata {
  
  val version = fieldOf[Int]
  val user_id = fieldOf[Int]
  val changeset_id = fieldOf[Long]
//  will be a Map
//  val key = fieldOf[String]
//  val value = fieldOf[String]
//  will be an Array
  val node_id = fieldOf[String]
  val sequence_id = fieldOf[Int]

  dependsOn { () => Seq(
      schedoscope.example.osm.stage.Ways(), 
      schedoscope.example.osm.stage.WayNodes()) }
  

  transformVia(() =>
    HiveTransformation(
      insertInto(
        this,
        queryFromResource("hiveql/processed/insert_ways.sql"),
        settings = Map("parquet.compression" -> "GZIP")
      ))
    .configureWith(defaultHiveQlParameters(this)))

  comment("View of ways, their referenced nodes and tags")

  storedAs(Parquet())
}
