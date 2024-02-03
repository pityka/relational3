package ra3.lang.syntax
import ra3.lang._
import ra3.lang.BufferIntExpr
import ra3.BufferInt

trait SyntaxBufferIntImpl {
  protected def arg0: BufferIntExpr
  def abs = Expr.makeOp1(ops.Op1.BufferAbsOpI)(arg0)
  def <=(arg1: BufferIntExpr) = Expr.makeOp2(ops.Op2.BufferLtEqOpII)(arg0, arg1)
  def <=(arg1: Int) = Expr.makeOp2(ops.Op2.BufferLtEqOpIcI)(arg0, arg1)
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

}
