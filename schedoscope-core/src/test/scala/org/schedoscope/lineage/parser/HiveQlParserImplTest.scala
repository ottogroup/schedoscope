package org.schedoscope.lineage.parser

import java.io.ByteArrayInputStream

import org.apache.calcite.avatica.util.Casing
import org.apache.calcite.sql.{SqlCall, SqlKind, SqlSelect}
import org.scalatest.{FlatSpec, Matchers}

class HiveQlParserImplTest extends FlatSpec with Matchers {
  "The HiveQlParserImpl" should "parse the <=> operator correctly" in {
    val sql = "SELECT * FROM a WHERE x <=> y"
    val stream = new ByteArrayInputStream(sql.getBytes)
    val parser = new HiveQlParserImpl(stream)
    parser.setIdentifierMaxLength(255)
    parser.setUnquotedCasing(Casing.UNCHANGED)

    val sqlNode = parser.parseSqlStmtEof
    val select = sqlNode.asInstanceOf[SqlSelect]
    val where = select.getWhere.asInstanceOf[SqlCall]
    where.getOperator.getKind should be(SqlKind.EQUALS)
  }
}
