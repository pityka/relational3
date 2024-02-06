package ra3.lang.syntax
import ra3.lang._
import ra3.{Buffer, Segment}

trait SyntaxColumnImpl {
  protected def arg0: Expr {
    type T = Either[Buffer, Seq[Segment]]
  }

  def unnamed = ra3.lang.Expr
    .BuiltInOp1(arg0, ops.Op1.MkUnnamedColumnSpecChunk)
    .asInstanceOf[Expr { type T = ColumnSpec }]

  def as(arg1: Expr { type T = String }) = ra3.lang.Expr
    .BuiltInOp2(arg0, arg1, ops.Op2.MkNamedColumnSpecChunk)
    .asInstanceOf[Expr { type T = ColumnSpec }]

}
