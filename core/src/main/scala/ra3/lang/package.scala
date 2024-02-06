package ra3
import cats.effect.IO
import tasks.TaskSystemComponents
package object lang {

  type Query = ra3.lang.Expr { type T <: ra3.lang.ReturnValue }
  type IntExpr = Expr { type T = Int }
  type StrExpr = Expr { type T = String }
  type BufferExpr = Expr { type T <: Buffer }
  type BufferIntExpr = Expr { type T = BufferInt }

  type DI32 = Either[BufferInt, Seq[SegmentInt]]

  type ColumnExpr = Expr {
    type T = Either[Buffer, Seq[Segment]]
  }
  type I32ColumnExpr = Expr {
    type T = DI32
  }
  type F64ColumnExpr = Expr {
    type T = Either[BufferDouble, Seq[SegmentDouble]]
  }

  type ReturnExpr = Expr { type T = ReturnValue }

  type GenericExpr[T0] = Expr { type T = T0 }

  implicit class SyntaxExpr[T0](a: Expr { type T = T0 })
      extends syntax.SyntaxExprImpl[T0] {
    // type T00 = T0
    protected val arg0 = a

  }
  implicit class SyntaxListExpr[T0](a: Expr { type T = List[T0] }) {
    // type T00 = T0
    protected val arg0 = a
    def ::(arg1: Expr { type T <: T0 }) =
      ra3.lang.Expr
        .BuiltInOp2(arg1, arg0, ops.Op2.Cons)
        .asInstanceOf[Expr { type T = List[T0] }]

  }
  implicit class SyntaxReturnExpr[T0](a: ReturnExpr) {
    // type T00 = T0
    protected val arg0 = a
    def where(arg1: I32ColumnExpr): ReturnExpr =
      Expr.makeOp2(ops.Op2.MkReturnWhere)(arg0, arg1)

  }
  implicit class SyntaxColumnInt(a: I32ColumnExpr)
      extends syntax.SyntaxI32ColumnImpl {
    protected def arg0 = a

  }
  implicit class SyntaxColumn(a: ColumnExpr) extends syntax.SyntaxColumnImpl {
    protected def arg0 = a

  }
  // implicit class SyntaxBufferInt(a: BufferIntExpr)
  //     extends syntax.SyntaxBufferIntImpl {
  //   protected def arg0 = a

  // }
  implicit class SyntaxInt(a: IntExpr) extends syntax.SyntaxIntImpl {
    protected def arg0 = a

  }
  implicit class SyntaxIntLit(a: Int) extends syntax.SyntaxIntImpl {
    protected def arg0 = Expr.LitNum(a)
    def lift = arg0
  }
  implicit class SyntaxStrLit(a: String) {
    protected def arg0 = Expr.LitStr(a)
    def lift = arg0
  }
  import scala.language.implicitConversions
  implicit def conversionIntLit(a: Int): Expr.LitNum = Expr.LitNum(a)
  implicit def conversionStrLit(a: String): Expr.LitStr = Expr.LitStr(a)
  implicit def conversionDI32(
      a: Expr { type T = DI32 }
  ): Expr { type T = ColumnSpec } = a.unnamed

  class Identifier[T0](id: Expr { type T = T0 }) {
    def apply[T1](
        body: Expr { type T = T0 } => Expr { type T = T1 }
    ): Expr { type T = T1 } =
      local(id)(body)
  }

  val star: Expr { type T = ColumnSpec } = ra3.lang.Expr.Star

  def local[T1](assigned: Expr)(body: Expr { type T = assigned.T } => Expr {
    type T = T1
  }): Expr { type T = T1 } = {
    val n = TagKey(new KeyTag)
    val b = body(Expr.Ident(n).as[assigned.T])

    Expr.Local(n, assigned, b).asInstanceOf[Expr { type T = T1 }]
  }
  def let[T1](assigned: Expr)(body: Expr { type T = assigned.T } => Expr {
    type T = T1
  }): Expr { type T = T1 } =
    local(assigned)(body)

  def global[T0](n: ColumnKey): Identifier[T0] = {
    val id = Expr.Ident(n).as[T0]
    new Identifier(id)
  }

  
 


  def select(args: Expr { type T = ColumnSpec }*): ReturnExpr = {
    Expr.makeOpStar(ops.MkSelect)(args: _*)
  }

  private[ra3] def evaluate(expr: Expr)(implicit
      tsc: TaskSystemComponents
  ): IO[Value[expr.T]] = expr.evalWith(Map.empty)
  private[ra3] def evaluate(expr: Expr, map: Map[Key, Value[_]])(implicit
      tsc: TaskSystemComponents
  ): IO[Value[expr.T]] =
    expr.evalWith(map)

  private[ra3] def bufferIfNeeded[
      B <: Buffer { type BufferType = B },
      S <: Segment { type SegmentType = S; type BufferType = B },
      C
  ](
      arg: Either[B, Seq[S]]
  )(implicit tsc: TaskSystemComponents): IO[B] =
    arg match {
      case Left(b) => IO.pure(b)
      case Right(s) =>
        ra3.Utils.bufferMultiple(s)
    }
}
