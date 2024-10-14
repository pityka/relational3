package ra3.lang.syntax

import ra3.lang.*
private[ra3] trait SyntaxStringImpl {
  protected def arg0: Expr[String]

  def unnamed = ra3.lang.Expr
    .BuiltInOp1(ops.Op1.MkUnnamedConstantStr)(arg0)

  infix def as(arg1: Expr[String]) = ra3.lang.Expr
    .BuiltInOp2(ops.Op2.MkNamedConstantStr)(arg0, arg1)
  infix def as(arg1: String): Expr[ColumnSpec[String]] = as(ra3.const(arg1))
}
