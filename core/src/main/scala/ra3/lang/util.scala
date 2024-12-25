package ra3.lang
import cats.effect.IO
import tasks.TaskSystemComponents
import ra3.tablelang.TableExpr
import ra3.*
object util {

  type IntExpr = Expr[Int]
  type StrExpr = Expr[String]
  type LongExpr = Expr[Long]
  type DoubleExpr = Expr[Double]

  // type ColumnExpr[T] = Expr[ Either[Ta, TaggedSegments]]
  type I32ColumnExpr = Expr[I32Var]
  type I64ColumnExpr = Expr[I64Var]
  type F64ColumnExpr = Expr[F64Var]
  type StrColumnExpr = Expr[StrVar]
  type InstColumnExpr = Expr[InstVar]

  private[ra3] def bufferIfNeededI32(
      arg: Either[BufferInt, Seq[SegmentInt]]
  )(implicit tsc: TaskSystemComponents): IO[BufferInt] =
    bufferIfNeeded(ColumnTag.I32)(arg)

  private[ra3] def bufferIfNeededF64(
      arg: Either[BufferDouble, Seq[SegmentDouble]]
  )(implicit tsc: TaskSystemComponents): IO[BufferDouble] =
    bufferIfNeeded(ColumnTag.F64)(arg)

  private[ra3] def bufferIfNeededI64(
      arg: Either[BufferLong, Seq[SegmentLong]]
  )(implicit tsc: TaskSystemComponents): IO[BufferLong] =
    bufferIfNeeded(ColumnTag.I64)(arg)

  private[ra3] def bufferIfNeededString(
      arg: Either[BufferString, Seq[SegmentString]]
  )(implicit tsc: TaskSystemComponents): IO[BufferString] =
    bufferIfNeeded(ColumnTag.StringTag)(arg)

  private[ra3] def bufferIfNeededInst(
      arg: Either[BufferInstant, Seq[SegmentInstant]]
  )(implicit tsc: TaskSystemComponents): IO[BufferInstant] =
    bufferIfNeeded(ColumnTag.Instant)(arg)

  private[ra3] def bufferIfNeeded(tag: ColumnTag)(
      arg: Either[tag.BufferType, Seq[tag.SegmentType]]
  )(implicit tsc: TaskSystemComponents): IO[tag.BufferType] =
    arg match {
      case Left(b) => IO.pure(b)
      case Right(s) =>
        ra3.Utils.bufferMultiple(tag)(s)
    }

  private[ra3] def bufferIfNeededWithPreconditionInst(
      arg: Either[BufferInstant, Seq[SegmentInstant]]
  )(prec: SegmentInstant => Boolean)(implicit
      tsc: TaskSystemComponents
  ): IO[Either[Int, BufferInstant]] =
    bufferIfNeededWithPrecondition(ColumnTag.Instant)(arg)(prec)

  private[ra3] def bufferIfNeededWithPreconditionString(
      arg: Either[BufferString, Seq[SegmentString]]
  )(prec: SegmentString => Boolean)(implicit
      tsc: TaskSystemComponents
  ): IO[Either[Int, BufferString]] =
    bufferIfNeededWithPrecondition(ColumnTag.StringTag)(arg)(prec)

  private[ra3] def bufferIfNeededWithPreconditionI32(
      arg: Either[BufferInt, Seq[SegmentInt]]
  )(prec: SegmentInt => Boolean)(implicit
      tsc: TaskSystemComponents
  ): IO[Either[Int, BufferInt]] =
    bufferIfNeededWithPrecondition(ColumnTag.I32)(arg)(prec)

  private[ra3] def bufferIfNeededWithPreconditionI64(
      arg: Either[BufferLong, Seq[SegmentLong]]
  )(prec: SegmentLong => Boolean)(implicit
      tsc: TaskSystemComponents
  ): IO[Either[Int, BufferLong]] =
    bufferIfNeededWithPrecondition(ColumnTag.I64)(arg)(prec)

  private[ra3] def bufferIfNeededWithPreconditionF64(
      arg: Either[BufferDouble, Seq[SegmentDouble]]
  )(prec: SegmentDouble => Boolean)(implicit
      tsc: TaskSystemComponents
  ): IO[Either[Int, BufferDouble]] =
    bufferIfNeededWithPrecondition(ColumnTag.F64)(arg)(prec)

  private[ra3] def bufferIfNeededWithPrecondition(tag: ColumnTag)(
      arg: Either[tag.BufferType, Seq[tag.SegmentType]]
  )(
      prec: tag.SegmentType => Boolean
  )(implicit tsc: TaskSystemComponents): IO[Either[Int, tag.BufferType]] =
    arg match {
      case Left(b) => IO.pure(Right(b))
      case Right(s) =>
        if (s.exists(prec))
          ra3.Utils.bufferMultiple(tag)(s).map(Right(_))
        else IO.pure(Left(s.map(_.numElems).sum))
    }

  private[ra3] def local[T1, R](
      assigned: Expr[T1]
  )(body: Expr[T1] => Expr[R]): Expr[R] = {
    val n = ra3.lang.TagKey(new ra3.lang.KeyTag)
    val b = body(Expr.Ident[T1](n))

    Expr.Local(n, assigned, b)
  }

}
