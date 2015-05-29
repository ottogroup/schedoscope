package schedoscope.example.osm.stage

import org.schedoscope.dsl.View
import org.schedoscope.dsl.TextFile
import org.schedoscope.dsl.transformations.CopyFrom

case class Nodes() extends View {

  val id = fieldOf[Long]
  val version = fieldOf[Int]
  val user_id = fieldOf[Int]
  val tstamp = fieldOf[String]
  val longitude = fieldOf[Double]
  val latitude = fieldOf[Double]

  transformVia(() => CopyFrom("classpath://osm-data/nodes.txt", this))

  comment("Stage View for data from file nodes.txt")

  storedAs(TextFile(fieldTerminator = "\\t", lineTerminator = "\\n"))
}