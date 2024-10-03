package ra3.lang.syntax

import ra3.lang.*
import ra3.lang.util.*
private[ra3] trait SyntaxReturnExprImpl[T0 <: Tuple] {

  protected def arg0: Expr[ReturnValueTuple[T0]]

  infix def :*[T1](v:Expr[ColumnSpec[T1]]) = extend(v)
  def extend[T1](
      arg1: Expr[ColumnSpec[T1]]
  ): Expr[ReturnValueTuple[Tuple.Append[T0, T1]]] =
    Expr
      .makeOp2(new ops.Op2.ExtendReturn[T0, T1])(
        arg0,
        arg1
      )
  infix def :*[T1 <: Tuple](
      arg1: ra3.tablelang.Schema[T1]
  ): Expr[ReturnValueTuple[Tuple.Concat[T0, T1]]] = extend(arg1)
  def extend[T1 <: Tuple](
      arg1: ra3.tablelang.Schema[T1]
  ): Expr[ReturnValueTuple[Tuple.Concat[T0, T1]]] =
    Expr
      .makeOp2(new ops.Op2.ConcatReturn[T0, T1])(
        arg0,
        arg1.all
      )

  def where(arg1: I32ColumnExpr): Expr[ReturnValue[T0]] =
    Expr
      .makeOp2(new ops.Op2.MkReturnWhere[T0])(
        arg0,
        arg1
      )
}
