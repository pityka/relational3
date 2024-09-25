package ra3.lang.syntax

import ra3.lang.*
private[ra3] trait SyntaxDoubleImpl {
  protected def arg0: Expr{type T = Double}
  
  def unnamed = ra3.lang.Expr
    .BuiltInOp1(arg0, ops.Op1.MkUnnamedConstantF64)
    .asInstanceOf[Expr { type T = ColumnSpec[Double] }]

  infix def as(arg1: Expr { type T = String }) = ra3.lang.Expr
    .BuiltInOp2(arg0, arg1, ops.Op2.MkNamedConstantF64)
    .asInstanceOf[Expr { type T = ColumnSpec[Double] }]
  infix def as(arg1: String): Expr { type T = ColumnSpec[Double] } = as(Expr.LitStr(arg1))
}