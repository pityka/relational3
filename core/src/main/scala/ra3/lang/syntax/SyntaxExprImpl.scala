package ra3.lang.syntax

import ra3.lang.*
private[ra3] trait SyntaxExprImpl[T0] {

  
  protected def arg0: Expr[T0]

  def list: Expr[List[T0]] = ra3.lang.Expr
    .BuiltInOp1(new ops.Op1.List1[T0])(arg0)

  def tap(arg1: StrExpr): Expr[T0] = ra3.lang.Expr
    .BuiltInOp2(new ops.Op2.Tap[T0])(arg0, arg1 )
  def tap(arg1: String): Expr[T0] = tap(ra3.const(arg1))

}
