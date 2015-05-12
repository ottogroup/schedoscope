package org.schedoscope.dsl



trait ViewDsl extends StructureDsl {

  def dependsOn[V <: View: Manifest](dsf: () => Seq[V]): Unit

  def dependsOn[V <: View: Manifest](df: () => V): () => V

  def transformVia(ft: () => Transformation): Unit

  def comment(aComment: String): Unit

  def storedAs(f: StorageFormat, additionalStoragePathPrefix: String = null, additionalStoragePathSuffix: String = null): Unit

  def asTableSuffix[P <: Parameter[_]](p: P): P

  def privacySensitive[P <: PrivacySensitive](ps: P): P = {
    ps.isPrivacySensitive = true
    ps
  }
}
