package ra3.lang.ops
import ra3.*
import ra3.lang.*
import ra3.DI32
import ra3.lang.util.*
import tasks.TaskSystemComponents
import cats.effect.IO
private[ra3] sealed trait Op2 {
  type A0
  type A1
  type T
  def op(a: A0, b: A1)(implicit tsc: TaskSystemComponents): IO[T]
}
private[ra3] sealed trait Op2Unserializable {
  type A0
  type A1
  type T
  def op(a: A0, b: A1)(implicit tsc: TaskSystemComponents): IO[T]
  def erase: Op2
}

private[ra3] object Op2 {

  case object ExtendReturnUntyped extends Op2 {
    // types here are fake
    type A0 = ra3.lang.ReturnValueTuple[EmptyTuple]
    type A1 = ra3.lang.ColumnSpec[Int]
    type T = ra3.lang.ReturnValueTuple[Tuple.Append[EmptyTuple, Int]]
    def op(a0: A0, a1: A1)(implicit tsc: TaskSystemComponents) =
      IO.pure(a0.extend(a1))
  }
  class ExtendReturn[T0 <: Tuple, T1] extends Op2Unserializable {

    def erase = ExtendReturnUntyped
    type A0 = ra3.lang.ReturnValueTuple[T0]
    type A1 = ra3.lang.ColumnSpec[T1]
    type T = ra3.lang.ReturnValueTuple[Tuple.Append[T0, T1]]
    def op(a0: A0, a1: A1)(implicit tsc: TaskSystemComponents) =
      IO.pure(a0.extend(a1))
  }

  class ConcatReturn[T0 <: Tuple, T1 <: Tuple] extends Op2Unserializable {

    def erase = ConcatReturnUntyped
    type A0 = ra3.lang.ReturnValueTuple[T0]
    type A1 = ra3.lang.ReturnValueTuple[T1]
    type T = ra3.lang.ReturnValueTuple[Tuple.Concat[T0, T1]]
    def op(a0: A0, a1: A1)(implicit tsc: TaskSystemComponents) =
      IO.pure(a0.concat(a1))
  }
  case object ConcatReturnUntyped extends Op2 {
    type A0 = ra3.lang.ReturnValueTuple[EmptyTuple]
    type A1 = ra3.lang.ReturnValueTuple[EmptyTuple]
    type T = ra3.lang.ReturnValueTuple[Tuple.Concat[EmptyTuple, EmptyTuple]]
    def op(a0: A0, a1: A1)(implicit tsc: TaskSystemComponents) =
      IO.pure(a0.concat(a1))
  }

  class Tap[B] extends Op2Unserializable {
    def erase = TapUntyped
    type A0 = B
    type A1 = String
    type T = A0
    def op(a: A0, b: A1)(implicit tsc: TaskSystemComponents) = IO {

      scribe.info(
        scribe.LogFeature.string2LoggableMessage(s"$b : ${a.toString}")
      )
      a
    }
  }
  case object TapUntyped extends Op2 {

    type A0 = Any
    type A1 = String
    type T = A0
    def op(a: A0, b: A1)(implicit tsc: TaskSystemComponents) = IO {

      scribe.info(
        scribe.LogFeature.string2LoggableMessage(s"$b : ${a.toString}")
      )
      a
    }
  }

  class MkReturnWhere[K<:Tuple] extends Op2Unserializable {
    def erase = MkReturnWhereUntyped
    type A0 = ra3.lang.ReturnValueTuple[K]
    type A1 = DI32
    type T = ReturnValueTuple[K]
    def op(a: A0, b: A1)(implicit tsc: TaskSystemComponents) =
      (a.filter match {
        case None => IO.pure(Some(b))
        case Some(f) =>
          for {
            f <- bufferIfNeededI32(f)
            b <- bufferIfNeededI32(b)
          } yield Some(Left(b.elementwise_&&(f)))
      }).map(f => a.replacePredicate(f))

  }
  case object MkReturnWhereUntyped extends Op2 {

    type A0 = ra3.lang.ReturnValueTuple[EmptyTuple]
    type A1 = DI32
    type T = A0
    def op(a: A0, b: A1)(implicit tsc: TaskSystemComponents) =
      (a.filter match {
        case None => IO.pure(Some(b))
        case Some(f) =>
          for {
            f <- bufferIfNeededI32(f)
            b <- bufferIfNeededI32(b)
          } yield Some(Left(b.elementwise_&&(f)))
      }).map(f => a.replacePredicate(f))

  }
  case object MkNamedColumnSpecChunkI32 extends Op2 {

    type A0 = ra3.DI32
    type A1 = String
    type T = NamedColumnChunkI32
    def op(a: A0, b: String)(implicit tsc: TaskSystemComponents) =
      IO.pure(
        NamedColumnChunkI32(
          a,
          b
        )
      )
  }
  case object MkNamedColumnSpecChunkF64 extends Op2 {

    type A0 = ra3.DF64
    type A1 = String
    type T = NamedColumnChunkF64
    def op(a: A0, b: String)(implicit tsc: TaskSystemComponents) =
      IO.pure(
        NamedColumnChunkF64(
          a,
          b
        )
      )
  }
  case object MkNamedColumnSpecChunkI64 extends Op2 {

    type A0 = ra3.DI64
    type A1 = String
    type T = NamedColumnChunkI64
    def op(a: A0, b: String)(implicit tsc: TaskSystemComponents) =
      IO.pure(
        NamedColumnChunkI64(
          a,
          b
        )
      )
  }
  case object MkNamedColumnSpecChunkString extends Op2 {

    type A0 = ra3.DStr
    type A1 = String
    type T = NamedColumnChunkStr
    def op(a: A0, b: String)(implicit tsc: TaskSystemComponents) =
      IO.pure(
        NamedColumnChunkStr(
          a,
          b
        )
      )
  }
  case object MkNamedColumnSpecChunkInst extends Op2 {

    type A0 = ra3.DInst
    type A1 = String
    type T = NamedColumnChunkInst
    def op(a: A0, b: String)(implicit tsc: TaskSystemComponents) =
      IO.pure(
        NamedColumnChunkInst(
          a,
          b
        )
      )
  }
  case object MkNamedConstantI32 extends Op2 {

    type A0 = Int
    type A1 = String
    type T = NamedConstantI32
    def op(a: A0, b: String)(implicit tsc: TaskSystemComponents) =
      IO.pure(NamedConstantI32(a, b))
  }
  case object MkNamedConstantI64 extends Op2 {

    type A0 = Long
    type A1 = String
    type T = NamedConstantI64
    def op(a: A0, b: String)(implicit tsc: TaskSystemComponents) =
      IO.pure(NamedConstantI64(a, b))
  }
  case object MkNamedConstantF64 extends Op2 {

    type A0 = Double
    type A1 = String
    type T = NamedConstantF64
    def op(a: A0, b: String)(implicit tsc: TaskSystemComponents) =
      IO.pure(NamedConstantF64(a, b))
  }
  case object MkNamedConstantStr extends Op2 {

    type A0 = String
    type A1 = String
    type T = NamedConstantString
    def op(a: A0, b: String)(implicit tsc: TaskSystemComponents) =
      IO.pure(NamedConstantString(a, b))
  }
  sealed trait ColumnOp2III extends Op2 {
    type A0 = ra3.DI32
    type A1 = ra3.DI32
    type T = ra3.DI32
  }
  sealed trait ColumnOp2ICII extends Op2 {
    type A0 = ra3.DI32
    type A1 = Int
    type T = ra3.DI32
  }
  sealed trait ColumnOp2StrCStrI extends Op2 {
    type A0 = ra3.DStr
    type A1 = String
    type T = ra3.DI32
  }
  sealed trait ColumnOp2StrStrI extends Op2 {
    type A0 = ra3.DStr
    type A1 = ra3.DStr
    type T = ra3.DI32
  }
  sealed trait ColumnOp2LCStrStr extends Op2 {
    type A0 = ra3.DI64
    type A1 = String
    type T = ra3.DStr
  }
  sealed trait ColumnOp2StrCStrStr extends Op2 {
    type A0 = ra3.DStr
    type A1 = String
    type T = ra3.DStr
  }
  sealed trait ColumnOp2StrStrStr extends Op2 {
    type A0 = ra3.DStr
    type A1 = ra3.DStr
    type T = ra3.DStr
  }
  sealed trait ColumnOp2StrCStrSetI extends Op2 {
    type A0 = ra3.DStr
    type A1 = Set[String]
    type T = ra3.DI32
  }

  sealed trait ColumnOp2ICISetI extends Op2 {
    type A0 = ra3.DI32
    type A1 = Set[Int]
    type T = ra3.DI32
  }
  sealed trait ColumnOp2DCDSetI extends Op2 {
    type A0 = ra3.DF64
    type A1 = Set[Double]
    type T = ra3.DI32
  }
  sealed trait ColumnOp2DDD extends Op2 {
    type A0 = ra3.DF64
    type A1 = ra3.DF64
    type T = ra3.DF64
  }
  sealed trait ColumnOp2DDI extends Op2 {
    type A0 = ra3.DF64
    type A1 = ra3.DF64
    type T = ra3.DI32
  }
  sealed trait ColumnOp2DcDI extends Op2 {
    type A0 = ra3.DF64
    type A1 = Double
    type T = ra3.DI32
  }
  sealed trait ColumnOp2DcStrStr extends Op2 {
    type A0 = ra3.DF64
    type A1 = String
    type T = ra3.DStr
  }
  sealed trait ColumnOp2IcStrStr extends Op2 {
    type A0 = ra3.DI32
    type A1 = String
    type T = ra3.DStr
  }
  sealed trait ColumnOp2InstcLInst extends Op2 {
    type A0 = ra3.DInst
    type A1 = Long
    type T = ra3.DInst
  }
  sealed trait ColumnOp2InstInstI extends Op2 {
    type A0 = ra3.DInst
    type A1 = ra3.DInst
    type T = ra3.DI32
  }
  sealed trait ColumnOp2InstcLI extends Op2 {
    type A0 = ra3.DInst
    type A1 = Long
    type T = ra3.DI32
  }
  sealed trait ColumnOp2InstcStrI extends Op2 {
    type A0 = ra3.DInst
    type A1 = String
    type T = ra3.DI32
  }

  sealed trait ColumnOp2InstcIInst extends Op2 {
    type A0 = ra3.DInst
    type A1 = Int
    type T = ra3.DInst
  }
  sealed trait ColumnOp2LLI extends Op2 {
    type A0 = ra3.DI64
    type A1 = ra3.DI64
    type T = ra3.DI32
  }
  sealed trait ColumnOp2LcLI extends Op2 {
    type A0 = ra3.DI64
    type A1 = Long
    type T = ra3.DI32
  }

  case object ColumnEqOpII extends ColumnOp2III {
    def op(a: DI32, b: DI32)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededI32(a)
        b <- bufferIfNeededI32(b)
      } yield Left(a.elementwise_eq(b))

    }
  }
  case object ColumnEqOpIcI extends ColumnOp2ICII {
    def op(a: DI32, b: Int)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededWithPreconditionI32(a)((segment: SegmentInt) =>
          segment.statistic.mightEq(b)
        )
      } yield (a match {
        case Right(a) => Left(a.elementwise_eq(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }
  }
  case object ColumnLtEqOpII extends ColumnOp2III {
    def op(a: DI32, b: DI32)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededI32(a)
        b <- bufferIfNeededI32(b)
      } yield Left(a.elementwise_lteq(b))

    }
  }
  case object ColumnLtOpII extends ColumnOp2III {
    def op(a: DI32, b: DI32)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededI32(a)
        b <- bufferIfNeededI32(b)
      } yield Left(a.elementwise_lt(b))

    }
  }
  case object ColumnLtEqOpIcI extends ColumnOp2ICII {
    def op(a: DI32, b: Int)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededWithPreconditionI32(a)((segment: SegmentInt) =>
          segment.statistic.mightLtEq(b)
        )
      } yield (a match {
        case Right(a) => Left(a.elementwise_lteq(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }
  }
  case object ColumnLtOpIcI extends ColumnOp2ICII {
    def op(a: DI32, b: Int)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededWithPreconditionI32(a)((segment: SegmentInt) =>
          segment.statistic.mightLt(b)
        )
      } yield (a match {
        case Right(a) => Left(a.elementwise_lt(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }
  }
  case object ColumnGtEqOpII extends ColumnOp2III {
    def op(a: DI32, b: DI32)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededI32(a)
        b <- bufferIfNeededI32(b)
      } yield Left(a.elementwise_gteq(b))

    }
  }
  case object ColumnGtOpII extends ColumnOp2III {
    def op(a: DI32, b: DI32)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededI32(a)
        b <- bufferIfNeededI32(b)
      } yield Left(a.elementwise_gt(b))

    }
  }
  case object ColumnGtEqOpIcI extends ColumnOp2ICII {
    def op(a: DI32, b: Int)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededWithPreconditionI32(a)((segment: SegmentInt) =>
          segment.statistic.mightGtEq(b)
        )
      } yield (a match {
        case Right(a) => Left(a.elementwise_gteq(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }
  }
  case object ColumnGtOpIcI extends ColumnOp2ICII {
    def op(a: DI32, b: Int)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededWithPreconditionI32(a)((segment: SegmentInt) =>
          segment.statistic.mightGt(b)
        )
      } yield (a match {
        case Right(a) => Left(a.elementwise_gt(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }
  }
  case object ColumnEqOpStrcStr extends ColumnOp2StrCStrI {
    def op(a: DStr, b: String)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededWithPreconditionString(a)((segment: SegmentString) =>
          segment.statistic.mightEq(b)
        )
      } yield (a match {
        case Right(a) => Left(a.elementwise_eq(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }
  }
  case object ColumnMatchesOpStrcStr extends ColumnOp2StrCStrI {
    def op(a: DStr, b: String)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededString(a)
      } yield Left(a.elementwise_matches(b))
    }
  }
  case object ColumnContainedInOpStrcStrSet extends ColumnOp2StrCStrSetI {
    def op(a: DStr, b: Set[String])(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = {
      for {
        a <- bufferIfNeededWithPreconditionString(a)((segment: SegmentString) =>
          b.exists(b => segment.statistic.mightEq(b))
        )
      } yield (a match {
        case Right(a) => Left(a.elementwise_containedIn(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }
  }
  case object ColumnContainedInOpIcISet extends ColumnOp2ICISetI {
    def op(a: DI32, b: Set[Int])(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = {
      for {
        a <- bufferIfNeededWithPreconditionI32(a)((segment: SegmentInt) =>
          b.exists(b => segment.statistic.mightEq(b))
        )
      } yield (a match {
        case Right(a) => Left(a.elementwise_containedIn(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }
  }
  case object ColumnContainedInOpDcDSet extends ColumnOp2DCDSetI {
    def op(a: DF64, b: Set[Double])(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = {
      for {
        a <- bufferIfNeededWithPreconditionF64(a)((segment: SegmentDouble) =>
          b.exists(b => segment.statistic.mightEq(b))
        )
      } yield (a match {
        case Right(a) => Left(a.elementwise_containedIn(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }
  }

  sealed trait Op2III extends Op2 {
    type A0 = Int
    type A1 = Int
    type T = Int
  }

  case object AddOp extends Op2III {
    def op(a: Int, b: Int)(implicit tsc: TaskSystemComponents) = IO.pure(a + b)
  }
  case object MinusOp extends Op2III {
    def op(a: Int, b: Int)(implicit tsc: TaskSystemComponents) = IO.pure(a - b)
  }

  case object ColumnDivOpDD extends ColumnOp2DDD {
    def op(a: DF64, b: DF64)(implicit tsc: TaskSystemComponents): IO[DF64] = {
      for {
        a <- bufferIfNeededF64(a)
        b <- bufferIfNeededF64(b)
      } yield Left(a.elementwise_div(b))

    }
  }
  case object ColumnMulOpDD extends ColumnOp2DDD {
    def op(a: DF64, b: DF64)(implicit tsc: TaskSystemComponents): IO[DF64] = {
      for {
        a <- bufferIfNeededF64(a)
        b <- bufferIfNeededF64(b)
      } yield Left(a.elementwise_mul(b))

    }
  }
  case object ColumnAddOpDD extends ColumnOp2DDD {
    def op(a: DF64, b: DF64)(implicit tsc: TaskSystemComponents): IO[DF64] = {
      for {
        a <- bufferIfNeededF64(a)
        b <- bufferIfNeededF64(b)
      } yield Left(a.elementwise_add(b))

    }
  }
  case object ColumnSubtractOpDD extends ColumnOp2DDD {
    def op(a: DF64, b: DF64)(implicit tsc: TaskSystemComponents): IO[DF64] = {
      for {
        a <- bufferIfNeededF64(a)
        b <- bufferIfNeededF64(b)
      } yield Left(a.elementwise_subtract(b))

    }
  }

  case object ColumnLtEqOpDD extends ColumnOp2DDI {
    def op(a: DF64, b: DF64)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededF64(a)
        b <- bufferIfNeededF64(b)
      } yield Left(a.elementwise_lteq(b))

    }
  }
  case object ColumnLtEqOpDcD extends ColumnOp2DcDI {
    def op(a: DF64, b: Double)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededWithPreconditionF64(a)((segment: SegmentDouble) =>
          segment.statistic.mightLtEq(b)
        )
      } yield (a match {
        case Right(a) => Left(a.elementwise_lteq(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }

  }

  case object ColumnLtOpDD extends ColumnOp2DDI {
    def op(a: DF64, b: DF64)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededF64(a)
        b <- bufferIfNeededF64(b)
      } yield Left(a.elementwise_lt(b))

    }
  }
  case object ColumnLtOpDcD extends ColumnOp2DcDI {
    def op(a: DF64, b: Double)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededWithPreconditionF64(a)((segment: SegmentDouble) =>
          segment.statistic.mightLt(b)
        )
      } yield (a match {
        case Right(a) => Left(a.elementwise_lt(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }
  }
  case object ColumnGtEqOpDD extends ColumnOp2DDI {
    def op(a: DF64, b: DF64)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededF64(a)
        b <- bufferIfNeededF64(b)
      } yield Left(a.elementwise_gteq(b))

    }
  }
  case object ColumnGtEqOpDcD extends ColumnOp2DcDI {
    def op(a: DF64, b: Double)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededWithPreconditionF64(a)((segment: SegmentDouble) =>
          segment.statistic.mightGtEq(b)
        )
      } yield (a match {
        case Right(a) => Left(a.elementwise_gteq(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }
  }
  case object ColumnGtOpDD extends ColumnOp2DDI {
    def op(a: DF64, b: DF64)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededF64(a)
        b <- bufferIfNeededF64(b)
      } yield Left(a.elementwise_gt(b))

    }
  }
  case object ColumnGtOpDcD extends ColumnOp2DcDI {
    def op(a: DF64, b: Double)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededWithPreconditionF64(a)((segment: SegmentDouble) =>
          segment.statistic.mightGt(b)
        )
      } yield (a match {
        case Right(a) => Left(a.elementwise_gt(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }
  }
  case object ColumnEqOpDD extends ColumnOp2DDI {
    def op(a: DF64, b: DF64)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededF64(a)
        b <- bufferIfNeededF64(b)
      } yield Left(a.elementwise_eq(b))

    }
  }
  case object ColumnEqOpDcD extends ColumnOp2DcDI {
    def op(a: DF64, b: Double)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededWithPreconditionF64(a)((segment: SegmentDouble) =>
          segment.statistic.mightEq(b)
        )
      } yield (a match {
        case Right(a) => Left(a.elementwise_eq(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }
  }
  case object ColumnNEqOpDD extends ColumnOp2DDI {
    def op(a: DF64, b: DF64)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededF64(a)
        b <- bufferIfNeededF64(b)
      } yield Left(a.elementwise_neq(b))

    }
  }
  case object ColumnNEqOpDcD extends ColumnOp2DcDI {
    def op(a: DF64, b: Double)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededF64(a)
      } yield Left(a.elementwise_neq(b))

    }
  }

  case object ColumnPrintfOpDcStr extends ColumnOp2DcStrStr {
    def op(a: DF64, b: String)(implicit tsc: TaskSystemComponents): IO[DStr] = {
      for {
        a <- bufferIfNeededF64(a)
      } yield Left(a.elementwise_printf(b))

    }
  }
  case object ColumnPrintfOpIcStr extends ColumnOp2IcStrStr {
    def op(a: DI32, b: String)(implicit tsc: TaskSystemComponents): IO[DStr] = {
      for {
        a <- bufferIfNeededI32(a)
      } yield Left(a.elementwise_printf(b))

    }
  }

  case object ColumnNEqOpII extends ColumnOp2III {
    def op(a: DI32, b: DI32)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededI32(a)
        b <- bufferIfNeededI32(b)
      } yield Left(a.elementwise_neq(b))

    }
  }
  case object ColumnNEqOpIcI extends ColumnOp2ICII {
    def op(a: DI32, b: Int)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededI32(a)
      } yield Left(a.elementwise_neq(b))

    }
  }
  // case object ColumnNEqOpIcStr extends ColumnOp2IcStrI {
  //   def op(a: DI32, b: String)(implicit tsc: TaskSystemComponents): IO[DI32] = {
  //     for {
  //       a <- bufferIfNeeded(a)
  //       b <- bufferIfNeeded(b)
  //     } yield Left(a.elementwise_neq(b))

  //   }
  // }

  case object ColumnPlusOpInstcL extends ColumnOp2InstcLInst {
    def op(a: DInst, b: Long)(implicit tsc: TaskSystemComponents): IO[DInst] = {
      for {
        a <- bufferIfNeededInst(a)
      } yield Left(a.elementwise_plus(b))

    }
  }
  case object ColumnMinusOpInstcL extends ColumnOp2InstcLInst {
    def op(a: DInst, b: Long)(implicit tsc: TaskSystemComponents): IO[DInst] = {
      for {
        a <- bufferIfNeededInst(a)
      } yield Left(a.elementwise_minus(b))

    }
  }

  case object ColumnLtEqOpInstInst extends ColumnOp2InstInstI {
    def op(a: DInst, b: DInst)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededInst(a)
        b <- bufferIfNeededInst(b)
      } yield Left(a.elementwise_lteq(b))

    }
  }

  case object ColumnLtEqOpInstcStr extends ColumnOp2InstcStrI {
    def op(a: DInst, b: String)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = {
      for {
        a <- bufferIfNeededInst(a)
      } yield Left(
        a.elementwise_lteq(java.time.Instant.parse(b).toEpochMilli())
      )

    }
  }

  case object ColumnLtOpInstInst extends ColumnOp2InstInstI {
    def op(a: DInst, b: DInst)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededInst(a)
        b <- bufferIfNeededInst(b)
      } yield Left(a.elementwise_lt(b))

    }
  }

  case object ColumnLtOpInstcStr extends ColumnOp2InstcStrI {
    def op(a: DInst, b: String)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = {
      for {
        a <- bufferIfNeededInst(a)
      } yield Left(
        a.elementwise_lt(java.time.Instant.parse(b).toEpochMilli())
      )

    }
  }
  case object ColumnGtEqOpInstInst extends ColumnOp2InstInstI {
    def op(a: DInst, b: DInst)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededInst(a)
        b <- bufferIfNeededInst(b)
      } yield Left(a.elementwise_gteq(b))

    }
  }

  case object ColumnGtEqOpInstcStr extends ColumnOp2InstcStrI {
    def op(a: DInst, b: String)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = {
      for {
        a <- bufferIfNeededInst(a)
      } yield Left(
        a.elementwise_gteq(java.time.Instant.parse(b).toEpochMilli())
      )

    }
  }
  case object ColumnGtOpInstInst extends ColumnOp2InstInstI {
    def op(a: DInst, b: DInst)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededInst(a)
        b <- bufferIfNeededInst(b)
      } yield Left(a.elementwise_gt(b))

    }
  }

  case object ColumnGtOpInstcStr extends ColumnOp2InstcStrI {
    def op(a: DInst, b: String)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = {
      for {
        a <- bufferIfNeededInst(a)
      } yield Left(
        a.elementwise_gt(java.time.Instant.parse(b).toEpochMilli())
      )

    }
  }
  case object ColumnEqOpInstInst extends ColumnOp2InstInstI {
    def op(a: DInst, b: DInst)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededInst(a)
        b <- bufferIfNeededInst(b)
      } yield Left(a.elementwise_eq(b))

    }
  }

  case object ColumnEqOpInstcStr extends ColumnOp2InstcStrI {
    def op(a: DInst, b: String)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = {
      for {
        a <- bufferIfNeededInst(a)
      } yield Left(a.elementwise_eq(java.time.Instant.parse(b).toEpochMilli()))

    }
  }
  case object ColumnNEqOpInstInst extends ColumnOp2InstInstI {
    def op(a: DInst, b: DInst)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededInst(a)
        b <- bufferIfNeededInst(b)
      } yield Left(a.elementwise_neq(b))

    }
  }

  case object ColumnNEqOpInstcStr extends ColumnOp2InstcStrI {
    def op(a: DInst, b: String)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = {
      for {
        a <- bufferIfNeededInst(a)
      } yield Left(a.elementwise_neq(java.time.Instant.parse(b).toEpochMilli()))

    }
  }

  case object ColumnPlusSecondsOpInstcInt extends ColumnOp2InstcIInst {
    def op(a: DInst, b: Int)(implicit tsc: TaskSystemComponents): IO[DInst] = {
      for {
        a <- bufferIfNeededInst(a)
      } yield Left(a.elementwise_plus(b * 1000L))

    }
  }
  case object ColumnMinusSecondsOpInstcInt extends ColumnOp2InstcIInst {
    def op(a: DInst, b: Int)(implicit tsc: TaskSystemComponents): IO[DInst] = {
      for {
        a <- bufferIfNeededInst(a)
      } yield Left(a.elementwise_minus(b * 1000L))

    }
  }
  case object ColumnPlusDaysOpInstcInt extends ColumnOp2InstcIInst {
    def op(a: DInst, b: Int)(implicit tsc: TaskSystemComponents): IO[DInst] = {
      for {
        a <- bufferIfNeededInst(a)
      } yield Left(a.elementwise_plus(b * 1000L * 60L * 60L * 24L))

    }
  }
  case object ColumnMinusDaysOpInstcInt extends ColumnOp2InstcIInst {
    def op(a: DInst, b: Int)(implicit tsc: TaskSystemComponents): IO[DInst] = {
      for {
        a <- bufferIfNeededInst(a)
      } yield Left(a.elementwise_minus(b * 1000L * 60L * 60L * 24L))

    }
  }

  case object ColumnLtEqOpInstcL extends ColumnOp2InstcLI {
    def op(a: DInst, b: Long)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = {
      for {
        a <- bufferIfNeededInst(a)
      } yield Left(
        a.elementwise_lteq(b)
      )

    }
  }
  case object ColumnLtOpInstcL extends ColumnOp2InstcLI {
    def op(a: DInst, b: Long)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = {
      for {
        a <- bufferIfNeededInst(a)
      } yield Left(
        a.elementwise_lt(b)
      )

    }
  }
  case object ColumnGtEqOpInstcL extends ColumnOp2InstcLI {
    def op(a: DInst, b: Long)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = {
      for {
        a <- bufferIfNeededInst(a)
      } yield Left(
        a.elementwise_gteq(b)
      )

    }
  }
  case object ColumnGtOpInstcL extends ColumnOp2InstcLI {
    def op(a: DInst, b: Long)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = {
      for {
        a <- bufferIfNeededInst(a)
      } yield Left(
        a.elementwise_gt(b)
      )

    }
  }
  case object ColumnEqOpInstcL extends ColumnOp2InstcLI {
    def op(a: DInst, b: Long)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = {
      for {
        a <- bufferIfNeededInst(a)
      } yield Left(
        a.elementwise_eq(b)
      )

    }
  }
  case object ColumnNEqOpInstcL extends ColumnOp2InstcLI {
    def op(a: DInst, b: Long)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = {
      for {
        a <- bufferIfNeededInst(a)
      } yield Left(
        a.elementwise_neq(b)
      )

    }
  }

  case object ColumnNEqOpStrcStr extends ColumnOp2StrCStrI {
    def op(a: DStr, b: String)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededString(a)
      } yield Left(a.elementwise_neq(b))

    }
  }
  case object ColumnConcatOpStrcStr extends ColumnOp2StrCStrStr {
    def op(a: DStr, b: String)(implicit tsc: TaskSystemComponents): IO[DStr] = {
      for {
        a <- bufferIfNeededString(a)
      } yield Left(a.elementwise_concatenate(b))

    }
  }
  case object ColumnConcatOpStrStr extends ColumnOp2StrStrStr {
    def op(a: DStr, b: DStr)(implicit tsc: TaskSystemComponents): IO[DStr] = {
      for {
        a <- bufferIfNeededString(a)
        b <- bufferIfNeededString(b)
      } yield Left(a.elementwise_concatenate(b))

    }
  }

  case object ColumnAndOpII extends ColumnOp2III {
    def op(a: DI32, b: DI32)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededI32(a)
        b <- bufferIfNeededI32(b)
      } yield Left(a.elementwise_&&(b))

    }
  }
  case object ColumnOrOpII extends ColumnOp2III {
    def op(a: DI32, b: DI32)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededI32(a)
        b <- bufferIfNeededI32(b)
      } yield Left(a.`elementwise_||`(b))

    }
  }

  case object ColumnPrintfOpLcStr extends ColumnOp2LCStrStr {
    def op(a: DI64, b: String)(implicit tsc: TaskSystemComponents): IO[DStr] = {
      for {
        a <- bufferIfNeededI64(a)
      } yield Left(a.elementwise_printf(b))

    }
  }

  case object ColumnEqOpLL extends ColumnOp2LLI {
    def op(a: DI64, b: DI64)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededI64(a)
        b <- bufferIfNeededI64(b)
      } yield Left(a.elementwise_eq(b))

    }
  }
  case object ColumnEqOpLcL extends ColumnOp2LcLI {
    def op(a: DI64, b: Long)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededWithPreconditionI64(a)((segment: SegmentLong) =>
          segment.statistic.mightEq(b)
        )
      } yield (a match {
        case Right(a) => Left(a.elementwise_eq(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }
  }

//

  case object ColumnLtEqOpStrStr extends ColumnOp2StrStrI {
    def op(a: DStr, b: DStr)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededString(a)
        b <- bufferIfNeededString(b)
      } yield Left(a.elementwise_lteq(b))

    }
  }
  case object ColumnLtEqOpStrcStr extends ColumnOp2StrCStrI {
    def op(a: DStr, b: String)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededWithPreconditionString(a)((segment: SegmentString) =>
          segment.statistic.mightLtEq(b)
        )
      } yield (a match {
        case Right(a) => Left(a.elementwise_lteq(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }

  }
  case object ColumnGtEqOpStrStr extends ColumnOp2StrStrI {
    def op(a: DStr, b: DStr)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededString(a)
        b <- bufferIfNeededString(b)
      } yield Left(a.elementwise_gteq(b))

    }
  }
  case object ColumnGtEqOpStrcStr extends ColumnOp2StrCStrI {
    def op(a: DStr, b: String)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededWithPreconditionString(a)((segment: SegmentString) =>
          segment.statistic.mightGtEq(b)
        )
      } yield (a match {
        case Right(a) => Left(a.elementwise_gteq(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }

  }
  case object ColumnLtOpStrStr extends ColumnOp2StrStrI {
    def op(a: DStr, b: DStr)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededString(a)
        b <- bufferIfNeededString(b)
      } yield Left(a.elementwise_lt(b))

    }
  }
  case object ColumnLtOpStrcStr extends ColumnOp2StrCStrI {
    def op(a: DStr, b: String)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededWithPreconditionString(a)((segment: SegmentString) =>
          segment.statistic.mightLt(b)
        )
      } yield (a match {
        case Right(a) => Left(a.elementwise_lt(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }

  }
  case object ColumnGtOpStrStr extends ColumnOp2StrStrI {
    def op(a: DStr, b: DStr)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededString(a)
        b <- bufferIfNeededString(b)
      } yield Left(a.elementwise_gt(b))

    }
  }
  case object ColumnGtOpStrcStr extends ColumnOp2StrCStrI {
    def op(a: DStr, b: String)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededWithPreconditionString(a)((segment: SegmentString) =>
          segment.statistic.mightGt(b)
        )
      } yield (a match {
        case Right(a) => Left(a.elementwise_gt(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }

  }

  case object ColumnEqOpStrStr extends ColumnOp2StrStrI {
    def op(a: DStr, b: DStr)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededString(a)
        b <- bufferIfNeededString(b)
      } yield Left(a.elementwise_eq(b))

    }
  }
  case object ColumnNEqOpStrStr extends ColumnOp2StrStrI {
    def op(a: DStr, b: DStr)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeededString(a)
        b <- bufferIfNeededString(b)
      } yield Left(a.elementwise_neq(b))

    }
  }

}
