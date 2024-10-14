package ra3.lang.ops
import ra3.lang.*
import ra3.*
import tasks.TaskSystemComponents
import cats.effect.*
sealed trait OpAny {
  type A
  type T

  def op(a: List[A])(implicit tsc: TaskSystemComponents): IO[T]

}
sealed trait OpAnyUnserializable {
  def erased: OpAny
  type A
  type T

  def op(a: List[A])(implicit tsc: TaskSystemComponents): IO[T]

}
object OpAnyUnserializable {
  class MkReturnValueTuple[B <: Tuple] extends OpAnyUnserializable {

    def erased = OpAny.MkReturnValueTupleUntyped
    type A = ColumnSpec[Any]
    type T = ReturnValueTuple[B]
    def op(a: List[A])(implicit tsc: TaskSystemComponents) =
      IO.pure(ReturnValueTuple[B](a, None))

  }

}

object OpAny {

  case object MkReturnValueTupleUntyped extends OpAny {
    type A = ColumnSpec[Any]
    type T = ReturnValueTuple[EmptyTuple]
    def op(a: List[A])(implicit tsc: TaskSystemComponents) =
      IO.pure(ReturnValueTuple[EmptyTuple](a, None))

  }

}