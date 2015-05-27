package schedoscope.example.osm.stage

import org.schedoscope.dsl.View
import org.schedoscope.dsl.transformations.CopyFrom
import org.schedoscope.dsl.TextFile

case class WayNodes() extends View {
  val way_id = fieldOf[String]
  val node_id = fieldOf[String]
  val sequence_id = fieldOf[Int]

  transformVia(() => CopyFrom("classpath://osm-data/way_nodes.txt", this))

  comment("Stage View for data from file way_nodes.txt")

  storedAs(TextFile(fieldTerminator = "\\t", lineTerminator = "\\n"))
}
