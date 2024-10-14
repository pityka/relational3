package ra3.lang.syntax

import ra3.lang.*
import ra3.lang.util.StrExpr
private[ra3] trait SyntaxExprImpl[T0] {

  protected def arg0: Expr[T0]

  def tap(arg1: StrExpr): Expr[T0] = ra3.lang.Expr
    .BuiltInOp2US(new ops.Op2.Tap[T0])(arg0, arg1)
  def tap(arg1: String): Expr[T0] = tap(ra3.const(arg1))

 

}
