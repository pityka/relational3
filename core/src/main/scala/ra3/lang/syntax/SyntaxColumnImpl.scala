package ra3.lang.syntax
import ra3.lang.*
import ra3.{Buffer, Segment}

private[ra3] trait SyntaxColumnImpl {
  protected def arg0: Expr {
    type T = Either[Buffer, Seq[Segment]]
  }


}
