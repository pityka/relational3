package ra3

/** Scala hack to represent generic types which are not Nothing
  *
  * This is a type class with two ambiguous instances predefined for Nothing
  */
final class NotNothing[T]

object NotNothing {
  implicit def good[T]: NotNothing[T] = null

  @scala.annotation.implicitAmbiguous("Specify generic type other than Nothing")
  implicit def wrong1: NotNothing[Nothing] = null
  implicit def wrong2: NotNothing[Nothing] = null
}
