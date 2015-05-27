package schedoscope.example.osm.stage

import org.schedoscope.dsl.views.Id
import org.schedoscope.dsl.View
import org.schedoscope.dsl.TextFile

class Nodes extends View with Id {
  val version = fieldOf[Integer]
  val user_id = fieldOf[Integer]
  val timestamp = fieldOf[String]
  val changeSetId = fieldOf[Integer]

  storedAs(TextFile(fieldTerminator = "\t"))
}
