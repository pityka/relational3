package ra3.lang.syntax
import ra3.lang._
import ra3.BufferInt

trait SyntaxI32ColumnImpl {
  protected def arg0: I32ColumnExpr
  def abs = Expr.makeOp1(ops.Op1.ColumnAbsOpI)(arg0)
  def isMissing = Expr.makeOp1(ops.Op1.ColumnIsMissingOpI)(arg0)
  def not = Expr.makeOp1(ops.Op1.ColumnNotOpI)(arg0)

  def toDouble = Expr.makeOp1(ops.Op1.ColumnToDoubleOpI)(arg0)

  def <=(arg1: I32ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnLtEqOpII)(arg0, arg1)
  def <=(arg1: Int) = Expr.makeOp2(ops.Op2.ColumnLtEqOpIcI)(arg0, arg1)
  def >=(arg1: I32ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnGtEqOpII)(arg0, arg1)
  def >=(arg1: Int) = Expr.makeOp2(ops.Op2.ColumnGtEqOpIcI)(arg0, arg1)
  def ===(arg1: I32ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnEqOpII)(arg0, arg1)
  def ===(arg1: Int) = Expr.makeOp2(ops.Op2.ColumnEqOpIcI)(arg0, arg1)
  def !==(arg1: I32ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnNEqOpII)(arg0, arg1)
  def !==(arg1: Int) = Expr.makeOp2(ops.Op2.ColumnNEqOpIcI)(arg0, arg1)
  def containedIn(arg1: Set[Int]) =
    Expr.makeOp2(ops.Op2.ColumnContainedInOpIcISet)(arg0, arg1)

  def &&(arg1: I32ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnAndOpII)(arg0, arg1)
  def ||(arg1: I32ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnOrOpII)(arg0, arg1)

  def printf(arg1: String) =
    Expr.makeOp2(ops.Op2.ColumnPrintfOpIcStr)(arg0, arg1)

  def count = Expr.makeOp3(ops.Op3.BufferCountInGroupsOpI)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def sum = Expr.makeOp3(ops.Op3.BufferSumGroupsOpII)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def min = Expr.makeOp3(ops.Op3.BufferMinGroupsOpI)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def max = Expr.makeOp3(ops.Op3.BufferMaxGroupsOpI)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def first = Expr.makeOp3(ops.Op3.BufferFirstGroupsOpIIi)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def hasMissing = Expr.makeOp3(ops.Op3.BufferHasMissingInGroupsOpI)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def all = Expr.makeOp3(ops.Op3.BufferAllInGroupsOpI)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def any = Expr.makeOp3(ops.Op3.BufferAnyInGroupsOpI)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def none = Expr.makeOp3(ops.Op3.BufferNoneInGroupsOpI)(
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
