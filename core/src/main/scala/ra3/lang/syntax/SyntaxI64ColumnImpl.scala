package ra3.lang.syntax
import ra3.lang._
import ra3.BufferInt

trait SyntaxI64ColumnImpl {
  protected def arg0: I64ColumnExpr
  def isMissing = Expr.makeOp1(ops.Op1.ColumnIsMissingOpL)(arg0)

  def toDouble = Expr.makeOp1(ops.Op1.ColumnToDoubleOpL)(arg0)



  def printf(arg1: String) =
    Expr.makeOp2(ops.Op2.ColumnPrintfOpLcStr)(arg0, arg1)

  def count = Expr.makeOp3(ops.Op3.BufferCountInGroupsOpL)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  // def sum = Expr.makeOp3(ops.Op3.BufferSumGroupsOpL)(
  //   arg0,
  //   ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
  //   ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  // )
  // def min = Expr.makeOp3(ops.Op3.BufferMinGroupsOpL)(
  //   arg0,
  //   ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
  //   ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  // )
  // def max = Expr.makeOp3(ops.Op3.BufferMaxGroupsOpL)(
  //   arg0,
  //   ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
  //   ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  // )
  def first = Expr.makeOp3(ops.Op3.BufferFirstGroupsOpL)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def hasMissing = Expr.makeOp3(ops.Op3.BufferHasMissingInGroupsOpL)(
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


  // def <=(arg1: I32ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnLtEqOpII)(arg0, arg1)
  // def <=(arg1: Int) = Expr.makeOp2(ops.Op2.ColumnLtEqOpIcI)(arg0, arg1)
  // def >=(arg1: I32ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnGtEqOpII)(arg0, arg1)
  // def >=(arg1: Int) = Expr.makeOp2(ops.Op2.ColumnGtEqOpIcI)(arg0, arg1)

  // def <(arg1: I32ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnLtOpII)(arg0, arg1)
  // def <(arg1: Int) = Expr.makeOp2(ops.Op2.ColumnLtOpIcI)(arg0, arg1)
  // def >(arg1: I32ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnGtOpII)(arg0, arg1)
  // def >(arg1: Int) = Expr.makeOp2(ops.Op2.ColumnGtOpIcI)(arg0, arg1)
  
  def ===(arg1: I64ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnEqOpLL)(arg0, arg1)
  def ===(arg1: Long) = Expr.makeOp2(ops.Op2.ColumnEqOpLcL)(arg0, arg1)
  // def !==(arg1: I32ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnNEqOpII)(arg0, arg1)
  // def !==(arg1: Int) = Expr.makeOp2(ops.Op2.ColumnNEqOpIcI)(arg0, arg1)
  // def containedIn(arg1: Set[Int]) =
  //   Expr.makeOp2(ops.Op2.ColumnContainedInOpIcISet)(arg0, arg1)

}
