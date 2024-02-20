package ra3.lang.ops
import cats.effect.IO
import tasks.TaskSystemComponents
import ra3._
import ra3.lang.bufferIfNeededWithPrecondition
private[lang] sealed trait Op1 {
  type A0
  type T

  def bufferBefore[
      B <: Buffer { type BufferType = B },
      S <: Segment { type SegmentType = S; type BufferType = B },
      C
  ](
      arg: Either[B, Seq[S]]
  )(fun: B => C)(implicit tsc: TaskSystemComponents) =
    arg match {
      case Left(b) => IO.pure(Left(fun(b)))
      case Right(s) =>
        ra3.Utils.bufferMultiple(s).map(b => Left(fun(b)))
    }
  def op(a: A0)(implicit tsc: TaskSystemComponents): IO[T]
}

private[lang] object Op1 {

  object List1 extends Op1 {
    type A0
    type T = List[A0]
    def op(a: A0)(implicit tsc: TaskSystemComponents) = IO.pure(List(a))
  }

  case object MkUnnamedColumnSpecChunk extends Op1 {
    type A0 = Either[Buffer, Seq[Segment]]
    type T = ra3.lang.UnnamedColumnChunk
    def op(a: A0)(implicit tsc: TaskSystemComponents) =
      IO.pure(ra3.lang.UnnamedColumnChunk(a))
  }
  case object MkUnnamedConstantI32 extends Op1 {
    type A0 = Int
    type T = ra3.lang.UnnamedConstantI32
    def op(a: A0)(implicit tsc: TaskSystemComponents) =
      IO.pure(ra3.lang.UnnamedConstantI32(a))
  }

  sealed trait Op1II extends Op1 {
    type A0 = Int
    type T = Int
  }

  case object AbsOp extends Op1II {
    def op(a: Int)(implicit tsc: TaskSystemComponents): IO[Int] =
      IO.pure(math.abs(a))
  }
  case object ToString extends Op1 {
    type A0 = Int
    type T = String
    def op(a: Int)(implicit tsc: TaskSystemComponents): IO[String] =
      IO.pure(a.toString)
  }

  sealed trait ColumnOp1II extends Op1 {
    type A0 = ra3.lang.DI32
    type T = ra3.lang.DI32
  }
  sealed trait ColumnOp1DI extends Op1 {
    type A0 = ra3.lang.DF64
    type T = ra3.lang.DI32
  }
  sealed trait ColumnOp1InstI extends Op1 {
    type A0 = ra3.lang.DInst
    type T = ra3.lang.DI32
  }
  sealed trait ColumnOp1InstD extends Op1 {
    type A0 = ra3.lang.DInst
    type T = ra3.lang.DF64
  }
  sealed trait ColumnOp1InstL extends Op1 {
    type A0 = ra3.lang.DInst
    type T = ra3.lang.DI64
  }
  sealed trait ColumnOp1InstInst extends Op1 {
    type A0 = ra3.lang.DInst
    type T = ra3.lang.DInst
  }
  sealed trait ColumnOp1DD extends Op1 {
    type A0 = ra3.lang.DF64
    type T = ra3.lang.DF64
  }
  sealed trait ColumnOp1ID extends Op1 {
    type A0 = ra3.lang.DI32
    type T = ra3.lang.DF64
  }
  sealed trait ColumnOp1StrI extends Op1 {
    type A0 = ra3.lang.DStr
    type T = ra3.lang.DI32
  }
  sealed trait ColumnOp1StrD extends Op1 {
    type A0 = ra3.lang.DStr
    type T = ra3.lang.DF64
  }
  sealed trait ColumnOp1StrL extends Op1 {
    type A0 = ra3.lang.DStr
    type T = ra3.lang.DI64
  }
  sealed trait ColumnOp1StrInst extends Op1 {
    type A0 = ra3.lang.DStr
    type T = ra3.lang.DInst
  }
  sealed trait ColumnOp1InstStr extends Op1 {
    type A0 = ra3.lang.DInst
    type T = ra3.lang.DStr
  }
  sealed trait ColumnOp1LI extends Op1 {
    type A0 = ra3.lang.DI64
    type T = ra3.lang.DI32
  }
  sealed trait ColumnOp1LD extends Op1 {
    type A0 = ra3.lang.DI64
    type T = ra3.lang.DF64
  }

  case object ColumnAbsOpI extends ColumnOp1II {
    def op(
        a: ra3.lang.DI32
    )(implicit tsc: TaskSystemComponents) = bufferBefore(a)(_.elementwise_abs)
  }
  case object ColumnNotOpI extends ColumnOp1II {
    def op(
        a: ra3.lang.DI32
    )(implicit tsc: TaskSystemComponents) = bufferBefore(a)(_.elementwise_not)
  }

  case object ColumnIsMissingOpL extends ColumnOp1LI {
    def op(
        a: ra3.lang.DI64
    )(implicit tsc: TaskSystemComponents) =
       for {
        a <- bufferIfNeededWithPrecondition(a)((segment: SegmentLong) =>
          segment.statistic.hasMissing
        )
      } yield (a match {
        case Right(a) => Left(a.elementwise_isMissing)
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
  }
  case object ColumnIsMissingOpD extends ColumnOp1DI {
    def op(
        a: ra3.lang.DF64
    )(implicit tsc: TaskSystemComponents) =
      for {
        a <- bufferIfNeededWithPrecondition(a)((segment: SegmentDouble) =>
          segment.statistic.hasMissing
        )
      } yield (a match {
        case Right(a) => Left(a.elementwise_isMissing)
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
  }
  case object ColumnIsMissingOpStr extends ColumnOp1StrI {
    def op(
        a: ra3.lang.DStr
    )(implicit tsc: TaskSystemComponents) =
      for {
        a <- bufferIfNeededWithPrecondition(a)((segment: SegmentString) =>
          segment.statistic.hasMissing
        )
      } yield (a match {
        case Right(a) => Left(a.elementwise_isMissing)
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
  }
  case object ColumnAbsOpD extends ColumnOp1DD {
    def op(
        a: ra3.lang.DF64
    )(implicit tsc: TaskSystemComponents) = bufferBefore(a)(_.elementwise_abs)
  }
  case object ColumnRoundToDoubleOpD extends ColumnOp1DD {
    def op(
        a: ra3.lang.DF64
    )(implicit tsc: TaskSystemComponents) =
      bufferBefore(a)(_.elementwise_roundToDouble)
  }
  case object ColumnRoundToIntOpD extends ColumnOp1DI {
    def op(
        a: ra3.lang.DF64
    )(implicit tsc: TaskSystemComponents) =
      bufferBefore(a)(_.elementwise_roundToInt)
  }

  case object ColumnIsMissingOpI extends ColumnOp1II {
    def op(
        a: ra3.lang.DI32
    )(implicit tsc: TaskSystemComponents) =
      for {
        a <- bufferIfNeededWithPrecondition(a)((segment: SegmentInt) =>
          segment.statistic.hasMissing
        )
      } yield (a match {
        case Right(a) => Left(a.elementwise_isMissing)
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
  }
  case object ColumnToDoubleOpI extends ColumnOp1ID {
    def op(
        a: ra3.lang.DI32
    )(implicit tsc: TaskSystemComponents) =
      bufferBefore(a)(_.elementwise_toDouble)
  }

  case object ColumnIsMissingOpInst extends ColumnOp1InstI {
    def op(
        a: ra3.lang.DInst
    )(implicit tsc: TaskSystemComponents) =
      for {
        a <- bufferIfNeededWithPrecondition(a)((segment: SegmentInstant) =>
          segment.statistic.hasMissing
        )
      } yield (a match {
        case Right(a) => Left(a.elementwise_isMissing)
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
  }
  case object ColumnToDoubleOpInst extends ColumnOp1InstD {
    def op(
        a: ra3.lang.DInst
    )(implicit tsc: TaskSystemComponents) =
      bufferBefore(a)(_.elementwise_toDouble)
  }
  case object ColumnToLongOpInst extends ColumnOp1InstL {
    def op(
        a: ra3.lang.DInst
    )(implicit tsc: TaskSystemComponents) =
      bufferBefore(a)(_.elementwise_toLong)
  }
  case object ColumnYearsOpInst extends ColumnOp1InstI {
    def op(
        a: ra3.lang.DInst
    )(implicit tsc: TaskSystemComponents) = bufferBefore(a)(_.elementwise_years)
  }
  case object ColumnMonthsOpInst extends ColumnOp1InstI {
    def op(
        a: ra3.lang.DInst
    )(implicit tsc: TaskSystemComponents) =
      bufferBefore(a)(_.elementwise_months)
  }
  case object ColumnDaysOpInst extends ColumnOp1InstI {
    def op(
        a: ra3.lang.DInst
    )(implicit tsc: TaskSystemComponents) = bufferBefore(a)(_.elementwise_days)
  }
  case object ColumnHoursOpInst extends ColumnOp1InstI {
    def op(
        a: ra3.lang.DInst
    )(implicit tsc: TaskSystemComponents) = bufferBefore(a)(_.elementwise_hours)
  }
  case object ColumnMinutesOpInst extends ColumnOp1InstI {
    def op(
        a: ra3.lang.DInst
    )(implicit tsc: TaskSystemComponents) =
      bufferBefore(a)(_.elementwise_minutes)
  }
  case object ColumnSecondsOpInst extends ColumnOp1InstI {
    def op(
        a: ra3.lang.DInst
    )(implicit tsc: TaskSystemComponents) =
      bufferBefore(a)(_.elementwise_seconds)
  }

  case object ColumnNanosecondsOpInst extends ColumnOp1InstI {
    def op(
        a: ra3.lang.DInst
    )(implicit tsc: TaskSystemComponents) =
      bufferBefore(a)(_.elementwise_nanoseconds)
  }
  case object ColumnRoundToYearOpInst extends ColumnOp1InstInst {
    def op(
        a: ra3.lang.DInst
    )(implicit tsc: TaskSystemComponents) =
      bufferBefore(a)(_.elementwise_roundToYear)
  }
  case object ColumnRoundToMonthOpInst extends ColumnOp1InstInst {
    def op(
        a: ra3.lang.DInst
    )(implicit tsc: TaskSystemComponents) =
      bufferBefore(a)(_.elementwise_roundToMonth)
  }
  case object ColumnRoundToDayOpInst extends ColumnOp1InstInst {
    def op(
        a: ra3.lang.DInst
    )(implicit tsc: TaskSystemComponents) =
      bufferBefore(a)(_.elementwise_roundToDay)
  }
  case object ColumnRoundToHourOpInst extends ColumnOp1InstInst {
    def op(
        a: ra3.lang.DInst
    )(implicit tsc: TaskSystemComponents) =
      bufferBefore(a)(_.elementwise_roundToHours)
  }

  case object ColumnParseI32OpStr extends ColumnOp1StrI {
    def op(
        a: ra3.lang.DStr
    )(implicit tsc: TaskSystemComponents) =
      bufferBefore(a)(_.elementwise_parseInt)
  }
  case object ColumnParseF64OpStr extends ColumnOp1StrD {
    def op(
        a: ra3.lang.DStr
    )(implicit tsc: TaskSystemComponents) =
      bufferBefore(a)(_.elementwise_parseDouble)
  }
  case object ColumnParseI64OpStr extends ColumnOp1StrL {
    def op(
        a: ra3.lang.DStr
    )(implicit tsc: TaskSystemComponents) =
      bufferBefore(a)(_.elementwise_parseLong)
  }
  case object ColumnParseInstOpStr extends ColumnOp1StrInst {
    def op(
        a: ra3.lang.DStr
    )(implicit tsc: TaskSystemComponents) =
      bufferBefore(a)(_.elementwise_parseInstant)
  }
  case object ColumnToISOOpInst extends ColumnOp1InstStr {
    def op(
        a: ra3.lang.DInst
    )(implicit tsc: TaskSystemComponents) = bufferBefore(a)(_.elementwise_toISO)
  }
   case object ColumnToDoubleOpL extends ColumnOp1LD {
    def op(
        a: ra3.lang.DI64
    )(implicit tsc: TaskSystemComponents) =
      bufferBefore(a)(_.elementwise_toDouble)
  }

}
