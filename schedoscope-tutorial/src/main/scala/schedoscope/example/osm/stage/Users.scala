package schedoscope.example.osm.stage

import org.schedoscope.dsl.View
import org.schedoscope.dsl.transformations.CopyFrom
import org.schedoscope.dsl.TextFile
import org.schedoscope.dsl.views.Id

case class Users() extends View
  with Id {
  val name = fieldOf[String]

  transformVia(() => CopyFrom("classpath://osm-data/users.txt", this))

  comment("Stage View for data from file users.txt")

  storedAs(TextFile(fieldTerminator = "\\t", lineTerminator = "\\n"))
}