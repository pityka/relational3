package ra3.lang.ops
import ra3.lang.*
import ra3.*
import tasks.TaskSystemComponents
import cats.effect.*
sealed trait OpAny {
  type A
  type T

  def op(a: A)(implicit tsc: TaskSystemComponents): IO[T]

}

object OpAny {

  object MkReturnValueStar extends OpAny {
    type A = Seq[ColumnSpec[Any]]
    type T = ReturnValueList[Any]
    def op(a: A)(implicit tsc: TaskSystemComponents) =
      IO.pure(ReturnValueList(a, None))

  }
}
