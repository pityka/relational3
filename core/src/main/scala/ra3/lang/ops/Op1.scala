package ra3.lang.ops
import cats.effect.IO
import tasks.TaskSystemComponents
import ra3.*
import ra3.lang.util.*
import ra3.lang.*
import ra3.Utils.*
import java.time.Instant

sealed trait Op1 {
  type A0
  type T

  def op(a: A0)(implicit tsc: TaskSystemComponents): IO[T]
}

object Op1 {

  case class MkNamedColumnSpecChunkI32(name: String) extends Op1 {

    type A0 = ra3.I32Var
    type T = NamedColumnChunkI32[String]
    def op(a: A0)(implicit tsc: TaskSystemComponents) =
      IO.pure(
        NamedColumnChunkI32[String](
          a,
          name
        )
      )
  }

  case class MkNamedColumnSpecChunkF64(name: String) extends Op1 {

    type A0 = ra3.F64Var
    type T = NamedColumnChunkF64[String]
    def op(a: A0)(implicit tsc: TaskSystemComponents) =
      IO.pure(
        NamedColumnChunkF64(
          a,
          name
        )
      )
  }
  case class MkNamedColumnSpecChunkI64(name: String) extends Op1 {

    type A0 = ra3.I64Var
    type T = NamedColumnChunkI64[String]
    def op(a: A0)(implicit tsc: TaskSystemComponents) =
      IO.pure(
        NamedColumnChunkI64(
          a,
          name
        )
      )
  }
  case class MkNamedColumnSpecChunkString(name: String) extends Op1 {

    type A0 = ra3.StrVar
    type T = NamedColumnChunkStr[String]
    def op(a: A0)(implicit tsc: TaskSystemComponents) =
      IO.pure(
        NamedColumnChunkStr(
          a,
          name
        )
      )
  }
  case class MkNamedColumnSpecChunkInst(name: String) extends Op1 {

    type A0 = ra3.InstVar
    type T = NamedColumnChunkInst[String]
    def op(a: A0)(implicit tsc: TaskSystemComponents) =
      IO.pure(
        NamedColumnChunkInst(
          a,
          name
        )
      )
  }
  case class MkNamedConstantI32(name: String) extends Op1 {

    type A0 = Int
    type T = NamedConstantI32[String]
    def op(a: A0)(implicit tsc: TaskSystemComponents) =
      IO.pure(NamedConstantI32(a, name))
  }
  case class MkNamedConstantI64(name: String) extends Op1 {

    type A0 = Long
    type T = NamedConstantI64[String]
    def op(a: A0)(implicit tsc: TaskSystemComponents) =
      IO.pure(NamedConstantI64(a, name))
  }
  case class MkNamedConstantF64(name: String) extends Op1 {

    type A0 = Double
    type T = NamedConstantF64[String]
    def op(a: A0)(implicit tsc: TaskSystemComponents) =
      IO.pure(NamedConstantF64(a, name))
  }
  case class MkNamedConstantStr(name: String) extends Op1 {

    type A0 = String
    type T = NamedConstantString[String]
    def op(a: A0)(implicit tsc: TaskSystemComponents) =
      IO.pure(NamedConstantString(a, name))
  }

  // case object DynamicUnnamed extends Op1 {
  //   type A0 = Any
  //   type T = ra3.lang.ColumnSpec[Any]
  //   def op(a: A0)(implicit tsc: TaskSystemComponents) =
  //     IO.pure({
  //       val p = (a.asInstanceOf[Primitives])
  //       p match {
  //         case e: Instant => ra3.lang.UnnamedConstantInstant(e)
  //         case e: String  => ra3.lang.UnnamedConstantString(e)
  //         case e: Double  => ra3.lang.UnnamedConstantF64(e)
  //         case e: Long    => ra3.lang.UnnamedConstantI64(e)
  //         case e: Int     => ra3.lang.UnnamedConstantI32(e)
  //         case Left(buffer) =>
  //           buffer.asInstanceOf[Buffer] match {
  //             case e: BufferDouble  => ra3.lang.UnnamedColumnChunkF64(Left(e))
  //             case e: BufferInt     => ra3.lang.UnnamedColumnChunkI32(Left(e))
  //             case e: BufferLong    => ra3.lang.UnnamedColumnChunkI64(Left(e))
  //             case e: BufferInstant => ra3.lang.UnnamedColumnChunkInst(Left(e))
  //             case e: BufferString  => ra3.lang.UnnamedColumnChunkStr(Left(e))
  //           }
  //         case Right(segments) =>
  //           segments.asInstanceOf[Seq[Segment]].head match {
  //             case _: SegmentDouble =>
  //               ra3.lang.UnnamedColumnChunkF64(
  //                 Right(segments.asInstanceOf[Seq[SegmentDouble]])
  //               )
  //             case _: SegmentInt =>
  //               ra3.lang.UnnamedColumnChunkI32(
  //                 Right(segments.asInstanceOf[Seq[SegmentInt]])
  //               )
  //             case _: SegmentLong =>
  //               ra3.lang.UnnamedColumnChunkI64(
  //                 Right(segments.asInstanceOf[Seq[SegmentLong]])
  //               )
  //             case _: SegmentString =>
  //               ra3.lang.UnnamedColumnChunkStr(
  //                 Right(segments.asInstanceOf[Seq[SegmentString]])
  //               )
  //             case _: SegmentInstant =>
  //               ra3.lang.UnnamedColumnChunkInst(
  //                 Right(segments.asInstanceOf[Seq[SegmentInstant]])
  //               )
  //           }
  //       }

  //     })
  // }

  // case object MkUnnamedColumnSpecChunkI32 extends Op1 {
  //   type A0 = DI32
  //   type T = ra3.lang.UnnamedColumnChunkI32
  //   def op(a: A0)(implicit tsc: TaskSystemComponents) =
  //     IO.pure(ra3.lang.UnnamedColumnChunkI32(a))
  // }
  // case object MkUnnamedColumnSpecChunkI64 extends Op1 {
  //   type A0 = DI64
  //   type T = ra3.lang.UnnamedColumnChunkI64
  //   def op(a: A0)(implicit tsc: TaskSystemComponents) =
  //     IO.pure(ra3.lang.UnnamedColumnChunkI64(a))
  // }
  // case object MkUnnamedColumnSpecChunkF64 extends Op1 {
  //   type A0 = DF64
  //   type T = ra3.lang.UnnamedColumnChunkF64
  //   def op(a: A0)(implicit tsc: TaskSystemComponents) =
  //     IO.pure(ra3.lang.UnnamedColumnChunkF64(a))
  // }
  // case object MkUnnamedColumnSpecChunkStr extends Op1 {
  //   type A0 = DStr
  //   type T = ra3.lang.UnnamedColumnChunkStr
  //   def op(a: A0)(implicit tsc: TaskSystemComponents) =
  //     IO.pure(ra3.lang.UnnamedColumnChunkStr(a))
  // }
  // case object MkUnnamedColumnSpecChunkInst extends Op1 {
  //   type A0 = DInst
  //   type T = ra3.lang.UnnamedColumnChunkInst
  //   def op(a: A0)(implicit tsc: TaskSystemComponents) =
  //     IO.pure(ra3.lang.UnnamedColumnChunkInst(a))
  // }
  // case object MkUnnamedConstantI32 extends Op1 {
  //   type A0 = Int
  //   type T = ra3.lang.UnnamedConstantI32
  //   def op(a: A0)(implicit tsc: TaskSystemComponents) =
  //     IO.pure(ra3.lang.UnnamedConstantI32(a))
  // }
  // case object MkUnnamedConstantI64 extends Op1 {
  //   type A0 = Long
  //   type T = ra3.lang.UnnamedConstantI64
  //   def op(a: A0)(implicit tsc: TaskSystemComponents) =
  //     IO.pure(ra3.lang.UnnamedConstantI64(a))
  // }
  // case object MkUnnamedConstantF64 extends Op1 {
  //   type A0 = Double
  //   type T = ra3.lang.UnnamedConstantF64
  //   def op(a: A0)(implicit tsc: TaskSystemComponents) =
  //     IO.pure(ra3.lang.UnnamedConstantF64(a))
  // }

  // case object MkUnnamedConstantStr extends Op1 {
  //   type A0 = String
  //   type T = ra3.lang.UnnamedConstantString
  //   def op(a: A0)(implicit tsc: TaskSystemComponents) =
  //     IO.pure(ra3.lang.UnnamedConstantString(a))
  // }

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
    type A0 = ra3.I32Var
    type T = ra3.I32Var
  }
  sealed trait ColumnOp1DI extends Op1 {
    type A0 = ra3.F64Var
    type T = ra3.I32Var
  }
  sealed trait ColumnOp1InstI extends Op1 {
    type A0 = ra3.InstVar
    type T = ra3.I32Var
  }
  sealed trait ColumnOp1InstD extends Op1 {
    type A0 = ra3.InstVar
    type T = ra3.F64Var
  }
  sealed trait ColumnOp1InstL extends Op1 {
    type A0 = ra3.InstVar
    type T = ra3.I64Var
  }
  sealed trait ColumnOp1InstInst extends Op1 {
    type A0 = ra3.InstVar
    type T = ra3.InstVar
  }
  sealed trait ColumnOp1DD extends Op1 {
    type A0 = ra3.F64Var
    type T = ra3.F64Var
  }
  sealed trait ColumnOp1ID extends Op1 {
    type A0 = ra3.I32Var
    type T = ra3.F64Var
  }
  sealed trait ColumnOp1StrI extends Op1 {
    type A0 = ra3.StrVar
    type T = ra3.I32Var
  }
  sealed trait ColumnOp1StrD extends Op1 {
    type A0 = ra3.StrVar
    type T = ra3.F64Var
  }
  sealed trait ColumnOp1StrL extends Op1 {
    type A0 = ra3.StrVar
    type T = ra3.I64Var
  }
  sealed trait ColumnOp1StrInst extends Op1 {
    type A0 = ra3.StrVar
    type T = ra3.InstVar
  }
  sealed trait ColumnOp1InstStr extends Op1 {
    type A0 = ra3.InstVar
    type T = ra3.StrVar
  }
  sealed trait ColumnOp1LI extends Op1 {
    type A0 = ra3.I64Var
    type T = ra3.I32Var
  }
  sealed trait ColumnOp1LD extends Op1 {
    type A0 = ra3.I64Var
    type T = ra3.F64Var
  }
  sealed trait ColumnOp1LInst extends Op1 {
    type A0 = ra3.I64Var
    type T = ra3.InstVar
  }

  case object ColumnAbsOpI extends ColumnOp1II {
    def op(
        a: ra3.I32Var
    )(implicit tsc: TaskSystemComponents) =
      bufferBeforeI32(a.v)(_.elementwise_abs).map(I32Var(_)).logElapsed
  }
  case object ColumnNotOpI extends ColumnOp1II {
    def op(
        a: ra3.I32Var
    )(implicit tsc: TaskSystemComponents) =
      bufferBeforeI32(a.v)(_.elementwise_not).map(I32Var(_)).logElapsed
  }

  case object ColumnIsMissingOpL extends ColumnOp1LI {
    def op(
        a: ra3.I64Var
    )(implicit tsc: TaskSystemComponents) =
      (for {
        a <- bufferIfNeededWithPrecondition(ColumnTag.I64)(a.v)(
          (segment: SegmentLong) => segment.statistic.hasMissing
        )
      } yield I32Var(a match {
        case Right(a) => Left(a.elementwise_isMissing)
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })).logElapsed
  }
  case object ColumnIsMissingOpD extends ColumnOp1DI {
    def op(
        a: ra3.F64Var
    )(implicit tsc: TaskSystemComponents) =
      (for {
        a <- bufferIfNeededWithPrecondition(ColumnTag.F64)(a.v)(
          (segment: SegmentDouble) => segment.statistic.hasMissing
        )
      } yield I32Var(a match {
        case Right(a) => Left(a.elementwise_isMissing)
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })).logElapsed
  }
  case object ColumnIsMissingOpStr extends ColumnOp1StrI {
    def op(
        a: ra3.StrVar
    )(implicit tsc: TaskSystemComponents) =
      (for {
        a <- bufferIfNeededWithPrecondition(ColumnTag.StringTag)(a.v)(
          (segment: SegmentString) => segment.statistic.hasMissing
        )
      } yield I32Var(a match {
        case Right(a) => Left(a.elementwise_isMissing)
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })).logElapsed
  }
  case object ColumnAbsOpD extends ColumnOp1DD {
    def op(
        a: ra3.F64Var
    )(implicit tsc: TaskSystemComponents) =
      bufferBeforeF64(a.v)(_.elementwise_abs).map(F64Var(_)).logElapsed
  }
  case object ColumnRoundToDoubleOpD extends ColumnOp1DD {
    def op(
        a: ra3.F64Var
    )(implicit tsc: TaskSystemComponents) =
      bufferBeforeF64(a.v)(_.elementwise_roundToDouble)
        .map(F64Var(_))
        .logElapsed
  }
  case object ColumnRoundToIntOpD extends ColumnOp1DI {
    def op(
        a: ra3.F64Var
    )(implicit tsc: TaskSystemComponents) =
      bufferBeforeF64(a.v)(_.elementwise_roundToInt).map(I32Var(_)).logElapsed
  }

  case object ColumnIsMissingOpI extends ColumnOp1II {
    def op(
        a: ra3.I32Var
    )(implicit tsc: TaskSystemComponents) =
      (for {
        a <- bufferIfNeededWithPrecondition(ColumnTag.I32)(a.v)(
          (segment: SegmentInt) => segment.statistic.hasMissing
        )
      } yield I32Var(a match {
        case Right(a) => Left(a.elementwise_isMissing)
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })).logElapsed
  }
  case object ColumnToDoubleOpI extends ColumnOp1ID {
    def op(
        a: ra3.I32Var
    )(implicit tsc: TaskSystemComponents) =
      bufferBeforeI32(a.v)(_.elementwise_toDouble).map(F64Var(_)).logElapsed
  }

  case object ColumnIsMissingOpInst extends ColumnOp1InstI {
    def op(
        a: ra3.InstVar
    )(implicit tsc: TaskSystemComponents) =
      (for {
        a <- bufferIfNeededWithPrecondition(ColumnTag.Instant)(a.v)(
          (segment: SegmentInstant) => segment.statistic.hasMissing
        )
      } yield I32Var(a match {
        case Right(a) => Left(a.elementwise_isMissing)
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })).logElapsed
  }
  case object ColumnToDoubleOpInst extends ColumnOp1InstD {
    def op(
        a: ra3.InstVar
    )(implicit tsc: TaskSystemComponents) =
      bufferBeforeInstant(a.v)(_.elementwise_toDouble).map(F64Var(_)).logElapsed
  }
  case object ColumnToLongOpInst extends ColumnOp1InstL {
    def op(
        a: ra3.InstVar
    )(implicit tsc: TaskSystemComponents) =
      bufferBeforeInstant(a.v)(_.elementwise_toLong).map(I64Var(_)).logElapsed
  }
  case object ColumnYearsOpInst extends ColumnOp1InstI {
    def op(
        a: ra3.InstVar
    )(implicit tsc: TaskSystemComponents) =
      bufferBeforeInstant(a.v)(_.elementwise_years).map(I32Var(_)).logElapsed
  }
  case object ColumnMonthsOpInst extends ColumnOp1InstI {
    def op(
        a: ra3.InstVar
    )(implicit tsc: TaskSystemComponents) =
      bufferBeforeInstant(a.v)(_.elementwise_months).map(I32Var(_)).logElapsed
  }
  case object ColumnDaysOpInst extends ColumnOp1InstI {
    def op(
        a: ra3.InstVar
    )(implicit tsc: TaskSystemComponents) =
      bufferBeforeInstant(a.v)(_.elementwise_days).map(I32Var(_)).logElapsed
  }
  case object ColumnHoursOpInst extends ColumnOp1InstI {
    def op(
        a: ra3.InstVar
    )(implicit tsc: TaskSystemComponents) =
      bufferBeforeInstant(a.v)(_.elementwise_hours).map(I32Var(_)).logElapsed
  }
  case object ColumnMinutesOpInst extends ColumnOp1InstI {
    def op(
        a: ra3.InstVar
    )(implicit tsc: TaskSystemComponents) =
      bufferBeforeInstant(a.v)(_.elementwise_minutes).map(I32Var(_)).logElapsed
  }
  case object ColumnSecondsOpInst extends ColumnOp1InstI {
    def op(
        a: ra3.InstVar
    )(implicit tsc: TaskSystemComponents) =
      bufferBeforeInstant(a.v)(_.elementwise_seconds).map(I32Var(_)).logElapsed
  }

  case object ColumnNanosecondsOpInst extends ColumnOp1InstI {
    def op(
        a: ra3.InstVar
    )(implicit tsc: TaskSystemComponents) =
      bufferBeforeInstant(a.v)(_.elementwise_nanoseconds)
        .map(I32Var(_))
        .logElapsed
  }
  case object ColumnRoundToYearOpInst extends ColumnOp1InstInst {
    def op(
        a: ra3.InstVar
    )(implicit tsc: TaskSystemComponents) =
      bufferBeforeInstant(a.v)(_.elementwise_roundToYear)
        .map(InstVar(_))
        .logElapsed
  }
  case object ColumnRoundToMonthOpInst extends ColumnOp1InstInst {
    def op(
        a: ra3.InstVar
    )(implicit tsc: TaskSystemComponents) =
      bufferBeforeInstant(a.v)(_.elementwise_roundToMonth)
        .map(InstVar(_))
        .logElapsed
  }
  case object ColumnRoundToDayOpInst extends ColumnOp1InstInst {
    def op(
        a: ra3.InstVar
    )(implicit tsc: TaskSystemComponents) =
      bufferBeforeInstant(a.v)(_.elementwise_roundToDay)
        .map(InstVar(_))
        .logElapsed
  }
  case object ColumnRoundToHourOpInst extends ColumnOp1InstInst {
    def op(
        a: ra3.InstVar
    )(implicit tsc: TaskSystemComponents) =
      bufferBeforeInstant(a.v)(_.elementwise_roundToHours)
        .map(InstVar(_))
        .logElapsed
  }

  case object ColumnParseI32OpStr extends ColumnOp1StrI {
    def op(
        a: ra3.StrVar
    )(implicit tsc: TaskSystemComponents) =
      bufferBeforeString(a.v)(_.elementwise_parseInt).map(I32Var(_)).logElapsed
  }
  case object ColumnParseF64OpStr extends ColumnOp1StrD {
    def op(
        a: ra3.StrVar
    )(implicit tsc: TaskSystemComponents) =
      bufferBeforeString(a.v)(_.elementwise_parseDouble)
        .map(F64Var(_))
        .logElapsed
  }
  case object ColumnParseI64OpStr extends ColumnOp1StrL {
    def op(
        a: ra3.StrVar
    )(implicit tsc: TaskSystemComponents) =
      bufferBeforeString(a.v)(_.elementwise_parseLong).map(I64Var(_)).logElapsed
  }
  case object ColumnParseInstOpStr extends ColumnOp1StrInst {
    def op(
        a: ra3.StrVar
    )(implicit tsc: TaskSystemComponents) =
      bufferBeforeString(a.v)(_.elementwise_parseInstant)
        .map(InstVar(_))
        .logElapsed
  }
  case object ColumnToISOOpInst extends ColumnOp1InstStr {
    def op(
        a: ra3.InstVar
    )(implicit tsc: TaskSystemComponents) =
      bufferBeforeInstant(a.v)(_.elementwise_toISO).map(StrVar(_)).logElapsed
  }
  case object ColumnToDoubleOpL extends ColumnOp1LD {
    def op(
        a: ra3.I64Var
    )(implicit tsc: TaskSystemComponents) =
      bufferBeforeI64(a.v)(_.elementwise_toDouble).map(F64Var(_)).logElapsed
  }
  case object ColumnToInstantEpochMilliOpL extends ColumnOp1LInst {
    def op(
        a: ra3.I64Var
    )(implicit tsc: TaskSystemComponents) =
      bufferBeforeI64(a.v)(_.elementwise_toInstantEpochMilli)
        .map(InstVar(_))
        .logElapsed
  }

}
