package ra3.lang

import ra3.*

case class ReturnValueTuple[A <: Tuple](
    private val list: List[ColumnSpec[Any]],
    filter: Option[DI32]
) {
  type MM = Tuple.Map[A, ra3.lang.Expr.DelayedIdent]
  def replacePredicate(i: Option[DI32]) = ReturnValueTuple[A](list, i)

  infix def :*[T](v: ColumnSpec[T]) = extend(v)

  def extend[T](v: ColumnSpec[T]) =
    ReturnValueTuple[Tuple.Append[A, T]](list ++ List(v), filter)
  inline def drop[N <: Int] = {
    val i = scala.compiletime.constValue[N]
    ReturnValueTuple[Tuple.Drop[A, N]](list.drop(i), filter)
  }

  def concat[B <: Tuple](b: ReturnValueTuple[B]) = {
    ReturnValueTuple[Tuple.Concat[A, B]](
      list ++ b.list,
      b.filter.orElse(filter)
    )
  }
}
object ReturnValueTuple {
  val empty = ReturnValueTuple[EmptyTuple](Nil, None)

  def list(r: ReturnValueTuple[?]): List[ColumnSpec[?]] = r.list
}
