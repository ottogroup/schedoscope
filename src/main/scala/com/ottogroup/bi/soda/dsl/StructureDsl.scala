package com.ottogroup.bi.soda.dsl

trait StructureDsl {
  protected def registerField(f: Field[_]): Unit

  def fieldOf[T: Manifest](orderWeight: Int, nameOverride: String) = {
    val f = Field[T](orderWeight, nameOverride)
    registerField(f)
    f
  }

  def fieldOf[T: Manifest](orderWeight: Int): Field[T] = fieldOf[T](orderWeight, null)

  def fieldOf[T: Manifest](nameOverride: String): Field[T] = fieldOf[T](100, nameOverride)

  def fieldOf[T: Manifest]: Field[T] = fieldOf[T](100, null)
}