package ra3
import cats.effect.IO
import tasks.TaskSystemComponents
import ra3.tablelang.TableExpr
package object lang {

  type Query = ra3.lang.Expr { type T <: ra3.lang.ReturnValue}
  
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

  private[ra3] type ReturnExpr1[A] = Expr { type T = ReturnValue1[A] }
  private[ra3] type ReturnExpr2[A,B] = Expr { type T = ReturnValue2[A,B] }
  type ColumnSpecExpr[A] = Expr { type T = ColumnSpec[A] }

  type ExprT[A] = Expr{ type T = A}

  private[ra3] type GenericExpr[T0] = Expr { type T = T0 }
  type DelayedIdent[T0] = Expr.DelayedIdent { type T = T0 }

  private[ra3] def global[T0](n: ColumnKey): Identifier[T0] = {
    val id = Expr.Ident(n).as[T0]
    new Identifier(id)
  }

  private[ra3] def evaluate(expr: Expr)(implicit
      tsc: TaskSystemComponents
  ): IO[Value[expr.T]] = expr.evalWith(Map.empty)
  private[ra3] def evaluate(expr: Expr, map: Map[Key, Value[?]])(implicit
      tsc: TaskSystemComponents
  ): IO[Value[expr.T]] =
    expr.evalWith(map)

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
  )(prec: SegmentInstant => Boolean)(implicit tsc: TaskSystemComponents):  IO[Either[Int, BufferInstant]] = 
    bufferIfNeededWithPrecondition(ColumnTag.Instant)(arg)(prec)

  private[ra3] def bufferIfNeededWithPreconditionString(
      arg: Either[BufferString, Seq[SegmentString]]
  )(prec: SegmentString => Boolean)(implicit tsc: TaskSystemComponents):  IO[Either[Int, BufferString]] = 
    bufferIfNeededWithPrecondition(ColumnTag.StringTag)(arg)(prec)

  private[ra3] def bufferIfNeededWithPreconditionI32(
      arg: Either[BufferInt, Seq[SegmentInt]]
  )(prec: SegmentInt => Boolean)(implicit tsc: TaskSystemComponents):  IO[Either[Int, BufferInt]] = 
    bufferIfNeededWithPrecondition(ColumnTag.I32)(arg)(prec)
    
  private[ra3] def bufferIfNeededWithPreconditionI64(
      arg: Either[BufferLong, Seq[SegmentLong]]
  )(prec: SegmentLong => Boolean)(implicit tsc: TaskSystemComponents):  IO[Either[Int, BufferLong]] = 
    bufferIfNeededWithPrecondition(ColumnTag.I64)(arg)(prec)

  private[ra3] def bufferIfNeededWithPreconditionF64(
      arg: Either[BufferDouble, Seq[SegmentDouble]]
  )(prec: SegmentDouble => Boolean)(implicit tsc: TaskSystemComponents):  IO[Either[Int, BufferDouble]] = 
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

  private[ra3] def local[T1](assigned: Expr)(body: Expr {
    type T = assigned.T
  } => Expr {
    type T = T1
  }): Expr { type T = T1 } = {
    val n = ra3.lang.TagKey(new ra3.lang.KeyTag)
    val b = body(Expr.Ident(n).as[assigned.T])

    Expr.Local(n, assigned, b).asInstanceOf[Expr { type T = T1 }]
  }





}
