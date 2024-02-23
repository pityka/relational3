package ra3
import cats.effect.IO
import tasks.TaskSystemComponents
import ra3.tablelang.TableExpr
package object lang {

  type Query = ra3.lang.Expr { type T <: ra3.lang.ReturnValue }
  type IntExpr = Expr { type T = Int }
  type StrExpr = Expr { type T = String }
  type LongExpr = Expr { type T = Long }
  type DoubleExpr = Expr { type T = Double }
  private[ra3] type BufferExpr = Expr { type T <: Buffer }

  type ColumnExpr = Expr {
    type T = Either[Buffer, Seq[Segment]]
  }
  type I32ColumnExpr = Expr {
    type T = DI32
  }
  type I64ColumnExpr = Expr {
    type T = DI64
  }
  type F64ColumnExpr = Expr {
    type T = Either[BufferDouble, Seq[SegmentDouble]]
  }
  type StrColumnExpr = Expr {
    type T = Either[BufferString, Seq[SegmentString]]
  }
  type InstColumnExpr = Expr {
    type T = Either[BufferInstant, Seq[SegmentInstant]]
  }

  private[ra3] type ReturnExpr = Expr { type T = ReturnValue }

  private[ra3] type GenericExpr[T0] = Expr { type T = T0 }
  type DelayedIdent[T0] = Expr.DelayedIdent { type T = T0 }

  private[ra3] def global[T0](n: ColumnKey): Identifier[T0] = {
    val id = Expr.Ident(n).as[T0]
    new Identifier(id)
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
  private[ra3] def bufferIfNeededWithPrecondition[
      B <: Buffer { type BufferType = B },
      S <: Segment { type SegmentType = S; type BufferType = B },
      C
  ](
      arg: Either[B, Seq[S]]
  )(
      prec: S => Boolean
  )(implicit tsc: TaskSystemComponents): IO[Either[Int, B]] =
    arg match {
      case Left(b) => IO.pure(Right(b))
      case Right(s) =>
        if (s.exists(prec))
          ra3.Utils.bufferMultiple(s).map(Right(_))
        else IO.pure(Left(s.map(_.numElems).sum))
    }

  private[ra3] def local[T1](assigned: Expr)(body: Expr {
    type T = assigned.T
  } => Expr {
    type T = T1
  }): Expr { type T = T1 } = {
    val n = ra3.lang.TagKey(new ra3.lang.KeyTag)
    val b = body(Expr.Ident(n).as[assigned.T])

    Expr.Local(n, assigned, b).asInstanceOf[Expr { type T = T1 }]
  }

  private[ra3] def local[T1](
      assigned: TableExpr
  )(body: TableExpr.Ident => TableExpr): TableExpr = {
    val n = ra3.tablelang.TagKey(new ra3.tablelang.KeyTag)
    val b = body(TableExpr.Ident(n))

    TableExpr.Local(n, assigned, b)
  }

}
