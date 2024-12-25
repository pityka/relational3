package ra3.lang.syntax

import ra3.lang.*
private[ra3] trait SyntaxDoubleImpl {
  protected def arg0: Expr[Double]

  infix def as[N <: String](arg1: N): Expr[ColumnSpec[arg1.type, Double]] =
    ra3.lang.Expr
      .BuiltInOp1(ops.Op1.MkNamedConstantF64(arg1))(arg0)
      .castToName[arg1.type]
}
