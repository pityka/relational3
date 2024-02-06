package ra3.lang.syntax

import ra3.lang._
trait SyntaxIntImpl {
  protected def arg0: IntExpr
  def +(arg1: IntExpr) = Expr.makeOp2(ops.Op2.AddOp)(arg0, arg1)
  def -(arg1: IntExpr) = Expr.makeOp2(ops.Op2.MinusOp)(arg0, arg1)
  def asString = Expr.makeOp1(ops.Op1.ToString)(arg0)
  def ifelse(t: IntExpr, f: IntExpr) = Expr.makeOp3(ops.Op3.IfElse)(arg0, t, f)

  def unnamed = ra3.lang.Expr
    .BuiltInOp1(arg0, ops.Op1.MkUnnamedConstantI32)
    .asInstanceOf[Expr { type T = ColumnSpec }]

  def as(arg1: Expr { type T = String }) = ra3.lang.Expr
    .BuiltInOp2(arg0, arg1, ops.Op2.MkNamedConstantI32)
    .asInstanceOf[Expr { type T = ColumnSpec }]
}
