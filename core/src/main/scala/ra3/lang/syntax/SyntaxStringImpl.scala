package ra3.lang.syntax

import ra3.lang.*
private[ra3] trait SyntaxStringImpl {
  protected def arg0: Expr[String]

  infix def as[N <: String](
      arg1: N
  ): Expr[ColumnSpec[arg1.type, String]] = ra3.lang.Expr
    .BuiltInOp1(ops.Op1.MkNamedConstantStr(arg1))(arg0)
    .castToName[arg1.type]
}
