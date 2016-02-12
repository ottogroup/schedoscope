package org.schedoscope.scheduler.states

import scala.language.implicitConversions
import org.schedoscope.dsl.View

/**
 * A view scheduling state machine might want to know who needs to be informed about a state transition.
 * These listeners may be of different type, which are subsumed and wrapped by this class.
 */
sealed abstract class ViewSchedulingStateListener

/**
 * A view depending on a given view with a state change.
 */
case class DependentView(view: View) extends ViewSchedulingStateListener

/**
 * Any other object wanting to know about the state change. E.g., actor refs.
 */
case class OtherListener(other: AnyRef) extends ViewSchedulingStateListener

object ViewSchedulingStateListener {
  /**
   * Implicit conversion of any object to its appropriate listener type.
   */
  implicit def toListener(a: AnyRef) = a match {
    case view: View => DependentView(view)
    case other      => OtherListener(other)
  }

  /**
   * An implicit conversion of a scheduling state listener to the wrapped class.
   */
  implicit def fromListener[T](d: ViewSchedulingStateListener) = (d match {
    case DependentView(view) => view
    case other               => other
  }).asInstanceOf[T]
}