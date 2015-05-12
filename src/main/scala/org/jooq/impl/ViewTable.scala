package org.jooq.impl

import java.util.Date

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.HashSet
import scala.collection.mutable.ListBuffer

import org.jooq.DataType
import org.jooq.Record
import org.jooq.impl.DSL.field
import org.jooq.impl.DSL.nullSafe

import com.openpojo.reflection.impl.PojoClassFactory
import org.schedoscope.dsl.FieldLike
import org.schedoscope.dsl.Structure
import org.schedoscope.dsl.View

object ViewTable {
  def viewOrStructureTable(s: Structure) = {
    if (classOf[View].isAssignableFrom(s.getClass()))
      new ViewTable(s.asInstanceOf[View])
    else {
      new StructureTable(s)
    }
  }

  def hiveQlPrimitiveDataType(m: Manifest[_]): DataType[_] = {
    val c = m.erasure
    if (c == classOf[java.lang.Integer])
      new DefaultDataType[java.lang.Integer](null, classOf[java.lang.Integer], "int")
    else if (c == classOf[java.lang.Long])
      new DefaultDataType[java.lang.Long](null, classOf[java.lang.Long], "bigint")
    else if (c == classOf[java.lang.Byte])
      new DefaultDataType[java.lang.Byte](null, classOf[java.lang.Byte], "tinyint")
    else if (c == classOf[java.lang.Boolean])
      new DefaultDataType[java.lang.Boolean](null, classOf[java.lang.Boolean], "boolean")
    else if (c == classOf[java.lang.Double])
      new DefaultDataType[java.lang.Double](null, classOf[java.lang.Double], "double")
    else if (c == classOf[java.lang.Float])
      new DefaultDataType[java.lang.Float](null, classOf[java.lang.Float], "float")
    else if (c == classOf[java.lang.String])
      new DefaultDataType[java.lang.String](null, classOf[java.lang.String], "string")
    else if (c == classOf[java.util.Date])
      new DefaultDataType[java.lang.String](null, classOf[java.lang.String], "string")
    else
      new DefaultDataType[java.lang.Byte](null, classOf[java.lang.Byte], "byte")
  }

  def hiveQlDataType(m: Manifest[_]): DataType[_] = {
    val ta = m.typeArguments

    if (!m.typeArguments.isEmpty)
      hiveQlDataType(m.typeArguments(0)).getArrayDataType()
    else
      hiveQlPrimitiveDataType(m)
  }

  def getElementType[T](array: ArrayDataType[T]): DataType[T] = {
    val elementTypeField = PojoClassFactory.getPojoClass(array.getClass()).getPojoFields().asScala.filter { f => f.getName() == "elementType" }.head

    elementTypeField.get(array).asInstanceOf[DataType[T]]
  }

  def get[T](array: org.jooq.Field[Array[T]], index: org.jooq.Field[java.lang.Integer]): org.jooq.Field[T] = {
    val resultType = getElementType(nullSafe(array).getDataType().asInstanceOf[ArrayDataType[T]])
    field("{0}[{1}]", resultType, nullSafe(array), nullSafe(index))
  }
}

class StructureTable(val structure: Structure) extends AbstractTable[Record](structure.n) {
  def as(alias: String) = new TableAlias[Record](this, alias, null, false)

  def as(alias: String, fieldAliases: String*) = new TableAlias(this, alias, fieldAliases.toArray, false)

  def getRecordType() = classOf[RecordImpl[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]]

  def allFields: Seq[FieldLike[_]] =
    structure.fields.asInstanceOf[Seq[FieldLike[_]]]

  def fields0() = {
    val dedupedFields = ListBuffer[FieldLike[_]]()
    val seenFieldNames = HashSet[String]()

    for (f <- allFields) {
      if (!seenFieldNames.contains(f.n)) {
        dedupedFields += f
        seenFieldNames += f.n
      }
    }

    val fs = new Fields[Record](dedupedFields.map { f => new TableFieldImpl(f.n, ViewTable.hiveQlDataType(f.t), this, null, null) }.toArray: _*)
    fs
  }

  override def toSQL(ctx: org.jooq.RenderContext) {
    if (structure.parentField != null) {
      ViewTable.viewOrStructureTable(structure.parentField.structure).toSQL(ctx)
      ctx.sql(".")
    }
    ctx.literal(structure.n)
  }
}

class ViewTable(view: View) extends StructureTable(view) {
  override def allFields =
    super.allFields ++ view.partitionParameters

  override def toSQL(ctx: org.jooq.RenderContext) {
    ctx.literal(view.dbName + "." + view.n)
  }
}