package ra3.lang.syntax

import ra3.lang.*
private[ra3] trait SyntaxLongImpl {
  protected def arg0: Expr[Long]

  infix def as[N <: String](arg1: N): Expr[ColumnSpec[arg1.type, Long]] =
    ra3.lang.Expr
      .BuiltInOp1(ops.Op1.MkNamedConstantI64(arg1))(arg0)
      .castToName[arg1.type]
}
