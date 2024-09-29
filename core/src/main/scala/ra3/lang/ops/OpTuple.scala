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

object OpAny {

 
  class MkReturnValueTuple[B<:Tuple] extends OpAny  {
    type A = ColumnSpec[Any]
    type T = ReturnValueTuple[B]
    def op(a: List[A])(implicit tsc: TaskSystemComponents) =
      IO.pure(ReturnValueTuple[B](a, None))

  }
}
