package com.ottogroup.bi.soda.dsl.transformations.sql

import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.dsl.Structure
import org.jooq.DSLContext
import org.jooq.Query
import org.jooq.impl.DSL._
import org.jooq.SQLDialect
import org.jooq.util.mysql.MySQLDSL
import org.jooq.conf.Settings
import org.jooq.impl.DefaultDSLContext
import com.ottogroup.bi.soda.dsl.Transformation
import com.ottogroup.bi.soda.dsl.FieldLike
import org.jooq.conf.RenderNameStyle
import java.util.Date
import org.jooq.impl.DSL
import org.jooq.DataType
import org.jooq.impl.ViewTable
import org.jooq.Param
import com.openpojo.reflection.impl.PojoClassFactory
import scala.collection.JavaConverters._
import org.jooq.impl.StructureTable
import scala.collection.mutable.HashMap

object HiveQlDsl {
  def dsl(specification: HiveDSLContext => Query) = {
    val context = new HiveDSLContext

    specification(context).getSQL(true)
  }

  implicit def t(s: Structure) = ViewTable.viewOrStructureTable(s)

  implicit def f[T <: Structure: Manifest](fl: FieldLike[T]) = {
    val table = t(fl.structure)
    table.field(fl.n).asInstanceOf[org.jooq.Field[java.lang.String]]
  }

  implicit def f(fl: FieldLike[Int]) = {
    val table = t(fl.structure)
    table.field(fl.n).asInstanceOf[org.jooq.Field[java.lang.Integer]]
  }

  implicit def f(fl: FieldLike[Float])(implicit d1: DummyImplicit) = {
    val table = t(fl.structure)
    table.field(fl.n).asInstanceOf[org.jooq.Field[java.lang.Float]]
  }

  implicit def f(fl: FieldLike[String])(implicit d1: DummyImplicit, d2: DummyImplicit) = {
    val table = t(fl.structure)
    table.field(fl.n).asInstanceOf[org.jooq.Field[java.lang.String]]
  }

  implicit def f(fl: FieldLike[Date])(implicit d1: DummyImplicit, d2: DummyImplicit, d3: DummyImplicit) = {
    val table = t(fl.structure)
    table.field(fl.n).asInstanceOf[org.jooq.Field[java.lang.String]]
  }

  implicit def f(fl: FieldLike[Double])(implicit d1: DummyImplicit, d2: DummyImplicit, d3: DummyImplicit, d4: DummyImplicit) = {
    val table = t(fl.structure)
    table.field(fl.n).asInstanceOf[org.jooq.Field[java.lang.Double]]
  }

  implicit def f(fl: FieldLike[Long])(implicit d1: DummyImplicit, d2: DummyImplicit, d3: DummyImplicit, d4: DummyImplicit, d5: DummyImplicit) = {
    val table = t(fl.structure)
    table.field(fl.n).asInstanceOf[org.jooq.Field[java.lang.Long]]
  }

  implicit def f(fl: FieldLike[Boolean])(implicit d1: DummyImplicit, d2: DummyImplicit, d3: DummyImplicit, d4: DummyImplicit, d5: DummyImplicit, d6: DummyImplicit) = {
    val table = t(fl.structure)
    table.field(fl.n).asInstanceOf[org.jooq.Field[java.lang.Boolean]]
  }

  implicit def f(fl: FieldLike[Byte])(implicit d1: DummyImplicit, d2: DummyImplicit, d3: DummyImplicit, d4: DummyImplicit, d5: DummyImplicit, d6: DummyImplicit, d7: DummyImplicit) = {
    val table = t(fl.structure)
    table.field(fl.n).asInstanceOf[org.jooq.Field[java.lang.Byte]]
  }

  implicit def f(fl: FieldLike[Seq[Float]])(implicit d1: DummyImplicit, d2: DummyImplicit, d3: DummyImplicit, d4: DummyImplicit, d5: DummyImplicit, d6: DummyImplicit, d7: DummyImplicit, d8: DummyImplicit) = {
    val table = t(fl.structure)
    table.field(fl.n).asInstanceOf[org.jooq.Field[Array[java.lang.Float]]]
  }

  implicit def f[T <: Seq[String]](fl: FieldLike[T])(implicit d1: DummyImplicit, d2: DummyImplicit, d3: DummyImplicit, d4: DummyImplicit, d5: DummyImplicit, d6: DummyImplicit, d7: DummyImplicit, d8: DummyImplicit, d9: DummyImplicit) = {
    val table = t(fl.structure)
    table.field(fl.n).asInstanceOf[org.jooq.Field[Array[java.lang.String]]]
  }

  implicit def f[T <: Seq[Date]](fl: FieldLike[T])(implicit d1: DummyImplicit, d2: DummyImplicit, d3: DummyImplicit, d4: DummyImplicit, d5: DummyImplicit, d6: DummyImplicit, d7: DummyImplicit, d8: DummyImplicit, d9: DummyImplicit, d10: DummyImplicit) = {
    val table = t(fl.structure)
    table.field(fl.n).asInstanceOf[org.jooq.Field[Array[java.lang.String]]]
  }

  implicit def f[T <: Seq[Double]](fl: FieldLike[T])(implicit d1: DummyImplicit, d2: DummyImplicit, d3: DummyImplicit, d4: DummyImplicit, d5: DummyImplicit, d6: DummyImplicit, d7: DummyImplicit, d8: DummyImplicit, d9: DummyImplicit, d10: DummyImplicit, d11: DummyImplicit) = {
    val table = t(fl.structure)
    table.field(fl.n).asInstanceOf[org.jooq.Field[Seq[java.lang.Double]]]
  }

  implicit def f[T <: Seq[Long]](fl: FieldLike[T])(implicit d1: DummyImplicit, d2: DummyImplicit, d3: DummyImplicit, d4: DummyImplicit, d5: DummyImplicit, d6: DummyImplicit, d7: DummyImplicit, d8: DummyImplicit, d9: DummyImplicit, d10: DummyImplicit, d11: DummyImplicit, d12: DummyImplicit) = {
    val table = t(fl.structure)
    table.field(fl.n).asInstanceOf[org.jooq.Field[Array[java.lang.Long]]]
  }

  implicit def f[T <: Seq[Boolean]](fl: FieldLike[T])(implicit d1: DummyImplicit, d2: DummyImplicit, d3: DummyImplicit, d4: DummyImplicit, d5: DummyImplicit, d6: DummyImplicit, d7: DummyImplicit, d8: DummyImplicit, d9: DummyImplicit, d10: DummyImplicit, d11: DummyImplicit, d12: DummyImplicit, d13: DummyImplicit) = {
    val table = t(fl.structure)
    table.field(fl.n).asInstanceOf[org.jooq.Field[java.lang.Boolean]]
  }

  implicit def f[T <: Seq[Byte]](fl: FieldLike[Seq[Byte]])(implicit d1: DummyImplicit, d2: DummyImplicit, d3: DummyImplicit, d4: DummyImplicit, d5: DummyImplicit, d6: DummyImplicit, d7: DummyImplicit, d8: DummyImplicit, d9: DummyImplicit, d10: DummyImplicit, d11: DummyImplicit, d12: DummyImplicit, d13: DummyImplicit, d14: DummyImplicit) = {
    val table = t(fl.structure)
    table.field(fl.n).asInstanceOf[org.jooq.Field[Array[java.lang.Byte]]]
  }

  def regexpReplace(field: org.jooq.Field[String], regularExpression: String, replacement: String) =
    function("regexp_replace", classOf[String], field, inline(regularExpression), inline(replacement))

  def get[T](array: org.jooq.Field[Array[T]], index: java.lang.Integer): org.jooq.Field[T] =
    get(array, value(index))

  def get[T](array: org.jooq.Field[Array[T]], index: org.jooq.Field[java.lang.Integer]): org.jooq.Field[T] =
    ViewTable.get(array, index)

  def value[T](value: T): Param[T] = {
    val ms = PojoClassFactory.getPojoClass(classOf[DSL]).getPojoMethods().asScala
    val m = ms.filter(m => m.getName().equals("val") && m.getParameterTypes().size == 1).head
    m.invoke(null, value.asInstanceOf[Object]).asInstanceOf[Param[T]]
  }
}

class HiveDSLContext extends DefaultDSLContext(SQLDialect.MARIADB) {
  configuration().settings().setRenderNameStyle(RenderNameStyle.QUOTED)
  configuration().settings().setRenderFormatted(true)
}