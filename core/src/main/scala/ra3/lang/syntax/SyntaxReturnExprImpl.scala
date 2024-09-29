package ra3.lang.syntax

import ra3.lang.*
private[ra3] trait SyntaxReturnExprImpl[T0<:Tuple] {

  
  protected def arg0: Expr[ReturnValueTuple[T0]]

  //   class ExtendReturn[T0<:Tuple,T1] extends Op2 {
  //   type A0 = ra3.lang.ReturnValueTuple[T0]
  //   type A1 = ra3.lang.ColumnSpec[T1]
  //   type T = ra3.lang.ReturnValueTuple[Tuple.Append[T0,T1]]
  //   def op(a0: A0, a1: A1)(implicit tsc: TaskSystemComponents) =
  //     IO.pure(a0.extend(a1))
  // }
  
  def extend[T1](arg1: Expr[ColumnSpec[T1]]) : Expr[ReturnValueTuple[Tuple.Append[T0,T1]]] =
     Expr
      .makeOp2(new ops.Op2.ExtendReturn[T0,T1])(
        arg0,
        arg1
      )

  def where(arg1: I32ColumnExpr): Expr[ReturnValue[T0]] =
    Expr
      .makeOp2(new ops.Op2.MkReturnWhere[T0])(
        arg0,
        arg1
      )
}
