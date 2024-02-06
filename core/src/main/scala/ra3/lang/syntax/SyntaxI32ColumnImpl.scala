package ra3.lang.syntax
import ra3.lang._
import ra3.BufferInt

trait SyntaxI32ColumnImpl {
  protected def arg0: I32ColumnExpr
  def abs = Expr.makeOp1(ops.Op1.ColumnAbsOpI)(arg0)
  def <=(arg1: I32ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnLtEqOpII)(arg0, arg1)
  def <=(arg1: Int) = Expr.makeOp2(ops.Op2.ColumnLtEqOpIcI)(arg0, arg1)
  def sum = Expr.makeOp3(ops.Op3.BufferSumGroupsOpII)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def first = Expr.makeOp3(ops.Op3.BufferFirstGroupsOpIIi)(
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
