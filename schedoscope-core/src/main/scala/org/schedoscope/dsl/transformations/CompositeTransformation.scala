package org.schedoscope.dsl.transformations

case class CompositeTransformation(firstThisTransformation: Transformation, thenThatTransformation: Transformation, firstTransformationIsDriving: Boolean = true) extends Transformation {
  
  def name = "composite"

  override def fileResourcesToChecksum =
    if (firstTransformationIsDriving)
      firstThisTransformation.fileResourcesToChecksum
    else
      firstThisTransformation.fileResourcesToChecksum ++ thenThatTransformation.fileResourcesToChecksum

  override def stringsToChecksum =
    if (firstTransformationIsDriving)
      firstThisTransformation.stringsToChecksum
    else
      firstThisTransformation.stringsToChecksum ++ thenThatTransformation.stringsToChecksum    
      
  description = s"Composite(${firstThisTransformation.description}, ${thenThatTransformation.description})"
}