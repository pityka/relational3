package ra3.lang.syntax

import ra3.lang.*
import ra3.lang.util.*
private[ra3] trait SyntaxIntImpl {
  protected def arg0: IntExpr
  def +(arg1: IntExpr) = Expr.makeOp2(ops.Op2.AddOp)(arg0, arg1)

  /* Predef.any2stringadd interferes with + :( */

  def ++(arg1: IntExpr) = Expr.makeOp2(ops.Op2.AddOp)(arg0, arg1)
  def plus(arg1: IntExpr) = Expr.makeOp2(ops.Op2.AddOp)(arg0, arg1)
  def add(arg1: IntExpr) = Expr.makeOp2(ops.Op2.AddOp)(arg0, arg1)

  def -(arg1: IntExpr) = Expr.makeOp2(ops.Op2.MinusOp)(arg0, arg1)
  def asString = Expr.makeOp1(ops.Op1.ToString)(arg0)

  infix def as[N <: String](arg1: N): Expr[ColumnSpec[arg1.type, Int]] =
    ra3.lang.Expr
      .BuiltInOp1(ops.Op1.MkNamedConstantI32(arg1))(arg0)
      .castToName[arg1.type]
}
