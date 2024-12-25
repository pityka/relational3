package ra3.lang

import ra3.*
import scala.NamedTuple.NamedTuple

case class ReturnValueTuple[N <: Tuple, V <: Tuple](
    private val list: List[ColumnSpec[?, Any]],
    filter: Option[DI32]
) {
  // type A = NamedTuple[N,V]
  // type MM = scala.NamedTuple.Map[A, ra3.lang.Expr.DelayedIdent]
  def replacePredicate(i: Option[DI32]) = ReturnValueTuple[N, V](list, i)

  infix inline def :*[N0, T](v: ColumnSpec[N0, T]) = extend[N0, T](v)

  inline def extend[N0, T](v: ColumnSpec[N0, T]) =
    ReturnValueTuple[Tuple.Append[N, N0], Tuple.Append[V, T]](
      list ++ List(v),
      filter
    )
  inline def drop[n <: Int] = {
    val i = scala.compiletime.constValue[n]
    ReturnValueTuple[Tuple.Drop[N, n], Tuple.Drop[V, n]](list.drop(i), filter)
  }

  def concat[BN <: Tuple, B <: Tuple](b: ReturnValueTuple[BN, B]) = {
    ReturnValueTuple[Tuple.Concat[N, BN], Tuple.Concat[V, B]](
      list ++ b.list,
      b.filter.orElse(filter)
    )
  }
}
object ReturnValueTuple {
  val empty = ReturnValueTuple[EmptyTuple, EmptyTuple](Nil, None)

  def list(r: ReturnValueTuple[?, ?]): List[ColumnSpec[?, ?]] = r.list
}
