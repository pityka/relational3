package ra3.lang.syntax

import ra3.lang._
trait SyntaxExprImpl[T0] { self: SyntaxExpr[T0] =>
  protected def arg0: Expr { type T = T0 }

  def list: Expr { type T = List[T0] } = ra3.lang.Expr
    .BuiltInOp1(arg0, ops.Op1.List1)
    .asInstanceOf[Expr { type T = List[T0] }]

  def tap(arg1: StrExpr): Expr { type T = T0 } = ra3.lang.Expr
    .BuiltInOp2(arg0, arg1, ops.Op2.Tap)
    .asInstanceOf[Expr { type T = T0 }]
}
