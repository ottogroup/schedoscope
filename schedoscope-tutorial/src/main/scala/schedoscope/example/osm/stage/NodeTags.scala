package schedoscope.example.osm.stage

import org.schedoscope.dsl.View
import org.schedoscope.dsl.transformations.CopyFrom
import org.schedoscope.dsl.TextFile

case class NodeTags() extends View {

  val node_id = fieldOf[Long]
  val key = fieldOf[String]
  val value = fieldOf[String]

  transformVia(() => CopyFrom("classpath://osm-data/node_tags.txt", this))

  comment("Stage View for data from file node_tags.txt")

  storedAs(TextFile(fieldTerminator = "\\t", lineTerminator = "\\n"))
}
