package ra3.lang.syntax

import ra3.lang._
private[ra3] trait SyntaxStringImpl {
  protected def arg0: Expr{type T = String}
  
  def unnamed = ra3.lang.Expr
    .BuiltInOp1(arg0, ops.Op1.MkUnnamedConstantStr)
    .asInstanceOf[Expr { type T = ColumnSpec }]

  def as(arg1: Expr { type T = String }) = ra3.lang.Expr
    .BuiltInOp2(arg0, arg1, ops.Op2.MkNamedConstantStr)
    .asInstanceOf[Expr { type T = ColumnSpec }]
  def as(arg1: String): Expr { type T = ColumnSpec } = as(Expr.LitStr(arg1))
}