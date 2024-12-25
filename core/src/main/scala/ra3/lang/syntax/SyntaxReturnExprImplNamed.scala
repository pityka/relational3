package ra3.lang.syntax

import ra3.lang.*
import ra3.lang.util.*
private[ra3] trait SyntaxReturnExprImplNamed[N <: Tuple, V <: Tuple] {
  type T0 = scala.NamedTuple.NamedTuple[N, V]
  protected def arg0: Expr[ReturnValueTuple[N, V]]

  @scala.annotation.targetName(":*ColumnSpec")
  infix def :*[N, T1](v: Expr[ColumnSpec[N, T1]]) = extend(v)

  def extend[N1, T1](
      arg1: Expr[ColumnSpec[N1, T1]]
  ): Expr[ReturnValueTuple[Tuple.Append[N, N1], Tuple.Append[V, T1]]] =
    Expr
      .makeOp2(new ops.Op2.ExtendReturn[N, N1, V, T1])(
        arg0,
        arg1
      )
  // infix def :*[N1<:Tuple,T1 <: Tuple](
  //     arg1: ra3.tablelang.Schema[N1,T1]
  // ): Expr[ReturnValueTuple[Tuple.Concat[N,N1],Tuple.Concat[V, T1]]] = extend(arg1)

  def concat[N1 <: Tuple, T1 <: Tuple](
      arg1: Expr[ReturnValueTuple[N1, T1]]
  ): Expr[ReturnValueTuple[Tuple.Concat[N, N1], Tuple.Concat[V, T1]]] =
    Expr
      .makeOp2(new ops.Op2.ConcatReturn[N, N1, V, T1])(
        arg0,
        arg1
      )

  def where(arg1: I32ColumnExpr): Expr[ReturnValueTuple[N, V]] =
    Expr
      .makeOp2(new ops.Op2.MkReturnWhere[N, V])(
        arg0,
        arg1
      )
}
