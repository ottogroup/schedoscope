package org.schedoscope.dsl

abstract class FieldLike[T: Manifest] extends Named {
  val t = manifest[T]

  var structure: Structure = null

  override def namingBase = {
    if ((structure != null) && (structure.nameOf(this).isDefined))
      structure.nameOf(this).get
    else
      t.runtimeClass.getSimpleName
  }

  def <=(v: T) = (this, v)
}

trait PrivacySensitive {
  var isPrivacySensitive = false
}

trait ValueCarrying[T] {
  var v: Option[T] = None
}

case class Field[T: Manifest](orderWeight: Long, nameOverride: String) extends FieldLike[T] with PrivacySensitive {
  override def namingBase = if (nameOverride != null) nameOverride else super.namingBase
}

object Field {
  implicit def s[T <: Structure: Manifest](fl: Field[T]) = {
    val s = manifest[T].erasure.newInstance().asInstanceOf[T]
    s.parentField = fl.asInstanceOf[FieldLike[Structure]]
    s
  }
  def v[T](f: Field[T], v: T) = f <= v
}

case class Parameter[T: Manifest](orderWeight: Long) extends FieldLike[T] with ValueCarrying[T] with PrivacySensitive {
  override def equals(a: Any): Boolean = {
    if (a.getClass != this.getClass()) {
      return false
    }
    val p = a.asInstanceOf[Parameter[T]]

    (p.t == this.t) && (p.v == this.v) && (p.n == this.n)
  }

  override def hashCode(): Int = {
    t.hashCode + 7 * v.hashCode()
  }

  override def toString() = if (v.isDefined) s"Parameter(${v.get})" else super.toString
}

object Parameter {
  private var parameterCount = 0L

  def newCount = this.synchronized {
    parameterCount += 1
    parameterCount
  }

  def apply[T: Manifest](): Parameter[T] = {
    val parameter = new Parameter[T](newCount)
    parameter
  }

  def asParameter[T: Manifest](v: T): Parameter[T] = {
    val f = Parameter[T]
    f.v = Some(v)
    f
  }

  def p[T: Manifest](v: Parameter[T]) = asParameter[T](v.v.get)

  def p[T: Manifest](v: T) = asParameter[T](v)
}