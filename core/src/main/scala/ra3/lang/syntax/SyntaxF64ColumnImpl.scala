package ra3.lang.syntax
import ra3.lang._
import ra3.BufferInt

trait SyntaxF64ColumnImpl {
  protected def arg0: F64ColumnExpr
  def isMissing = Expr.makeOp1(ops.Op1.ColumnIsMissingOpD)(arg0)
  def abs = Expr.makeOp1(ops.Op1.ColumnAbsOpD)(arg0)
  def roundToDouble = Expr.makeOp1(ops.Op1.ColumnRoundToDoubleOpD)(arg0)
  def roundToInt = Expr.makeOp1(ops.Op1.ColumnRoundToIntOpD)(arg0)
  def /(arg1: F64ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnDivOpDD)(arg0, arg1)
  def *(arg1: F64ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnMulOpDD)(arg0, arg1)
  def +(arg1: F64ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnAddOpDD)(arg0, arg1)
  def -(arg1: F64ColumnExpr) =
    Expr.makeOp2(ops.Op2.ColumnSubtractOpDD)(arg0, arg1)
  def <=(arg1: F64ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnLtEqOpDD)(arg0, arg1)
  def <=(arg1: Double) = Expr.makeOp2(ops.Op2.ColumnGtEqOpDcD)(arg0, arg1)
  def ===(arg1: F64ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnEqOpDD)(arg0, arg1)
  def ===(arg1: Double) = Expr.makeOp2(ops.Op2.ColumnEqOpDcD)(arg0, arg1)
  def !==(arg1: F64ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnNEqOpDD)(arg0, arg1)
  def !==(arg1: Double) = Expr.makeOp2(ops.Op2.ColumnNEqOpDcD)(arg0, arg1)
  def >=(arg1: F64ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnGtEqOpDD)(arg0, arg1)
  def >=(arg1: Double) = Expr.makeOp2(ops.Op2.ColumnGtEqOpDcD)(arg0, arg1)

  def printf(arg1: String) =
    Expr.makeOp2(ops.Op2.ColumnPrintfOpDcStr)(arg0, arg1)

  def mean = Expr.makeOp3(ops.Op3.BufferMeanGroupsOpDI)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def sum = Expr.makeOp3(ops.Op3.BufferSumGroupsOpDI)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def min = Expr.makeOp3(ops.Op3.BufferMinGroupsOpD)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def max = Expr.makeOp3(ops.Op3.BufferMaxGroupsOpD)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def hasMissing = Expr.makeOp3(ops.Op3.BufferHasMissingInGroupsOpD)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def count = Expr.makeOp3(ops.Op3.BufferCountGroupsOpDI)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def first = Expr.makeOp3(ops.Op3.BufferFirstGroupsOpDIi)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )

  def unnamed = ra3.lang.Expr
    .BuiltInOp1(arg0, ops.Op1.MkUnnamedColumnSpecChunk)
    .asInstanceOf[Expr { type T = ColumnSpec }]

  def as(arg1: Expr { type T = String }) = ra3.lang.Expr
    .BuiltInOp2(arg0, arg1, ops.Op2.MkNamedColumnSpecChunk)
    .asInstanceOf[Expr { type T = ColumnSpec }]

}