package ra3.lang.syntax

import ra3.lang._
private[ra3] trait SyntaxIntImpl {
  protected def arg0: IntExpr
  def +(arg1: IntExpr) = Expr.makeOp2(ops.Op2.AddOp)(arg0, arg1)

  /* Predef.any2stringadd interferes with + :( */

  def ++(arg1: IntExpr) = Expr.makeOp2(ops.Op2.AddOp)(arg0, arg1)
  def plus(arg1: IntExpr) = Expr.makeOp2(ops.Op2.AddOp)(arg0, arg1)
  def add(arg1: IntExpr) = Expr.makeOp2(ops.Op2.AddOp)(arg0, arg1)

  def -(arg1: IntExpr) = Expr.makeOp2(ops.Op2.MinusOp)(arg0, arg1)
  def asString = Expr.makeOp1(ops.Op1.ToString)(arg0)
  

  // def unnamed = ra3.lang.Expr
  //   .BuiltInOp1(arg0, ops.Op1.MkUnnamedConstantI32)
  //   .asInstanceOf[Expr { type T = ColumnSpec }]

  // def as(arg1: Expr { type T = String }) = ra3.lang.Expr
  //   .BuiltInOp2(arg0, arg1, ops.Op2.MkNamedConstantI32)
  //   .asInstanceOf[Expr { type T = ColumnSpec }]
  // def as(arg1: String): Expr { type T = ColumnSpec } = as(Expr.LitStr(arg1))
}
