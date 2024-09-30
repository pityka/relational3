package ra3.lang.syntax

import ra3.lang.*
private[ra3] trait SyntaxDoubleImpl {
  protected def arg0: Expr[Double]

  def unnamed = ra3.lang.Expr
    .BuiltInOp1(ops.Op1.MkUnnamedConstantF64)(arg0)

  infix def as(arg1: Expr[String]) = ra3.lang.Expr
    .BuiltInOp2(ops.Op2.MkNamedConstantF64)(arg0, arg1)
  infix def as(arg1: String): Expr[ColumnSpec[Double]] = as(ra3.const(arg1))
}
