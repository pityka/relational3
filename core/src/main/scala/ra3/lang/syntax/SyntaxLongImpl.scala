package ra3.lang.syntax

import ra3.lang.*
private[ra3] trait SyntaxLongImpl {
  protected def arg0: Expr{type T = Long}
  
  // def unnamed = ra3.lang.Expr
  //   .BuiltInOp1(arg0, ops.Op1.MkUnnamedConstantI64)
  //   .asInstanceOf[Expr { type T = ColumnSpec }]

  infix def as(arg1: Expr { type T = String }) = ra3.lang.Expr
    .BuiltInOp2(arg0, arg1, ops.Op2.MkNamedConstantI64)
    .asInstanceOf[Expr { type T = ColumnSpec[ra3.DI64] }]
  infix def as(arg1: String): Expr { type T = ColumnSpec[ra3.DI64] } = as(Expr.LitStr(arg1))
}