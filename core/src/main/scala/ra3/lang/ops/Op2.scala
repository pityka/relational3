package ra3.lang.ops
import ra3.*
import ra3.lang.*
import ra3.I32Var
import ra3.lang.util.*
import tasks.TaskSystemComponents
import cats.effect.IO
sealed trait Op2 {
  type A0
  type A1
  type T
  def op(a: A0, b: A1)(implicit tsc: TaskSystemComponents): IO[T]
}
sealed trait Op2Unserializable {
  type A0
  type A1
  type T
  def op(a: A0, b: A1)(implicit tsc: TaskSystemComponents): IO[T]
  def erase: Op2
}

object Op2 {

  case object ExtendReturnUntyped extends Op2 {
    // types here are fake
    type A0 = ra3.lang.ReturnValueTuple[EmptyTuple, EmptyTuple]
    type A1 = ra3.lang.ColumnSpec["", Int]
    type T = ra3.lang.ReturnValueTuple[Tuple.Append[
      EmptyTuple,
      ""
    ], Tuple.Append[EmptyTuple, Int]]
    def op(a0: A0, a1: A1)(implicit tsc: TaskSystemComponents) =
      IO.pure(a0.extend(a1))
  }
  class ExtendReturn[N0 <: Tuple, N1, T0 <: Tuple, T1]
      extends Op2Unserializable {

    def erase = ExtendReturnUntyped
    type A0 = ra3.lang.ReturnValueTuple[N0, T0]
    type A1 = ra3.lang.ColumnSpec[N1, T1]
    type T =
      ra3.lang.ReturnValueTuple[Tuple.Append[N0, N1], Tuple.Append[T0, T1]]
    def op(a0: A0, a1: A1)(implicit tsc: TaskSystemComponents) =
      IO.pure(a0.extend(a1))
  }

  class ConcatReturn[N0 <: Tuple, N1 <: Tuple, T0 <: Tuple, T1 <: Tuple]
      extends Op2Unserializable {

    def erase = ConcatReturnUntyped
    type A0 = ra3.lang.ReturnValueTuple[N0, T0]
    type A1 = ra3.lang.ReturnValueTuple[N1, T1]
    type T =
      ra3.lang.ReturnValueTuple[Tuple.Concat[N0, N1], Tuple.Concat[T0, T1]]
    def op(a0: A0, a1: A1)(implicit tsc: TaskSystemComponents) =
      IO.pure(a0.concat(a1))
  }
  case object ConcatReturnUntyped extends Op2 {
    type A0 = ra3.lang.ReturnValueTuple[EmptyTuple, EmptyTuple]
    type A1 = ra3.lang.ReturnValueTuple[EmptyTuple, EmptyTuple]
    type T = ra3.lang.ReturnValueTuple[Tuple.Concat[
      EmptyTuple,
      EmptyTuple
    ], Tuple.Concat[EmptyTuple, EmptyTuple]]
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

  class MkReturnWhere[N <: Tuple, K <: Tuple] extends Op2Unserializable {
    def erase = MkReturnWhereUntyped
    type A0 = ra3.lang.ReturnValueTuple[N, K]
    type A1 = I32Var
    type T = ReturnValueTuple[N, K]
    def op(a: A0, b: A1)(implicit tsc: TaskSystemComponents) =
      (a.filter match {
        case None => IO.pure(Some(b))
        case Some(f) =>
          for {
            f <- bufferIfNeededI32(f)
            b <- bufferIfNeededI32(b.v)
          } yield Some(I32Var(Left(b.elementwise_&&(f))))
      }).map(f => a.replacePredicate(f.map(_.v))).logElapsed

  }
  case object MkReturnWhereUntyped extends Op2 {

    type A0 = ra3.lang.ReturnValueTuple[EmptyTuple, EmptyTuple]
    type A1 = I32Var
    type T = A0
    def op(a: A0, b: A1)(implicit tsc: TaskSystemComponents) =
      (a.filter match {
        case None => IO.pure(Some(b))
        case Some(f) =>
          for {
            f <- bufferIfNeededI32(f)
            b <- bufferIfNeededI32(b.v)
          } yield Some(I32Var(Left(b.elementwise_&&(f))))
      }).map(f => a.replacePredicate(f.map(_.v))).logElapsed

  }

  sealed trait ColumnOp2III extends Op2 {
    type A0 = ra3.I32Var
    type A1 = ra3.I32Var
    type T = ra3.I32Var
  }
  sealed trait ColumnOp2ICII extends Op2 {
    type A0 = ra3.I32Var
    type A1 = Int
    type T = ra3.I32Var
  }
  sealed trait ColumnOp2StrCStrI extends Op2 {
    type A0 = ra3.StrVar
    type A1 = String
    type T = ra3.I32Var
  }
  sealed trait ColumnOp2StrStrI extends Op2 {
    type A0 = ra3.StrVar
    type A1 = ra3.StrVar
    type T = ra3.I32Var
  }
  sealed trait ColumnOp2LCStrStr extends Op2 {
    type A0 = ra3.I64Var
    type A1 = String
    type T = ra3.StrVar
  }
  sealed trait ColumnOp2StrCStrStr extends Op2 {
    type A0 = ra3.StrVar
    type A1 = String
    type T = ra3.StrVar
  }
  sealed trait ColumnOp2StrStrStr extends Op2 {
    type A0 = ra3.StrVar
    type A1 = ra3.StrVar
    type T = ra3.StrVar
  }
  sealed trait ColumnOp2StrCStrSetI extends Op2 {
    type A0 = ra3.StrVar
    type A1 = Set[String]
    type T = ra3.I32Var
  }

  sealed trait ColumnOp2ICISetI extends Op2 {
    type A0 = ra3.I32Var
    type A1 = Set[Int]
    type T = ra3.I32Var
  }
  sealed trait ColumnOp2DCDSetI extends Op2 {
    type A0 = ra3.F64Var
    type A1 = Set[Double]
    type T = ra3.I32Var
  }
  sealed trait ColumnOp2DDD extends Op2 {
    type A0 = ra3.F64Var
    type A1 = ra3.F64Var
    type T = ra3.F64Var
  }
  sealed trait ColumnOp2DDI extends Op2 {
    type A0 = ra3.F64Var
    type A1 = ra3.F64Var
    type T = ra3.I32Var
  }
  sealed trait ColumnOp2DcDI extends Op2 {
    type A0 = ra3.F64Var
    type A1 = Double
    type T = ra3.I32Var
  }
  sealed trait ColumnOp2DcStrStr extends Op2 {
    type A0 = ra3.F64Var
    type A1 = String
    type T = ra3.StrVar
  }
  sealed trait ColumnOp2IcStrStr extends Op2 {
    type A0 = ra3.I32Var
    type A1 = String
    type T = ra3.StrVar
  }
  sealed trait ColumnOp2InstcLInst extends Op2 {
    type A0 = ra3.InstVar
    type A1 = Long
    type T = ra3.InstVar
  }
  sealed trait ColumnOp2InstInstI extends Op2 {
    type A0 = ra3.InstVar
    type A1 = ra3.InstVar
    type T = ra3.I32Var
  }
  sealed trait ColumnOp2InstcLI extends Op2 {
    type A0 = ra3.InstVar
    type A1 = Long
    type T = ra3.I32Var
  }
  sealed trait ColumnOp2InstcStrI extends Op2 {
    type A0 = ra3.InstVar
    type A1 = String
    type T = ra3.I32Var
  }

  sealed trait ColumnOp2InstcIInst extends Op2 {
    type A0 = ra3.InstVar
    type A1 = Int
    type T = ra3.InstVar
  }
  sealed trait ColumnOp2LLI extends Op2 {
    type A0 = ra3.I64Var
    type A1 = ra3.I64Var
    type T = ra3.I32Var
  }
  sealed trait ColumnOp2LcLI extends Op2 {
    type A0 = ra3.I64Var
    type A1 = Long
    type T = ra3.I32Var
  }

  case object ColumnEqOpII extends ColumnOp2III {
    def op(a: I32Var, b: I32Var)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      (for {
        a <- bufferIfNeededI32(a.v)
        b <- bufferIfNeededI32(b.v)
      } yield I32Var(Left(a.elementwise_eq(b)))).logElapsed

    }
  }
  case object ColumnEqOpIcI extends ColumnOp2ICII {
    def op(a: I32Var, b: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      (for {
        a <- bufferIfNeededWithPreconditionI32(a.v)((segment: SegmentInt) =>
          segment.statistic.mightEq(b)
        )
      } yield I32Var(a match {
        case Right(a) => Left(a.elementwise_eq(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })).logElapsed
    }
  }
  case object ColumnLtEqOpII extends ColumnOp2III {
    def op(a: I32Var, b: I32Var)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      (for {
        a <- bufferIfNeededI32(a.v)
        b <- bufferIfNeededI32(b.v)
      } yield I32Var(Left(a.elementwise_lteq(b)))).logElapsed

    }
  }
  case object ColumnLtOpII extends ColumnOp2III {
    def op(a: I32Var, b: I32Var)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      (for {
        a <- bufferIfNeededI32(a.v)
        b <- bufferIfNeededI32(b.v)
      } yield I32Var(Left(a.elementwise_lt(b)))).logElapsed

    }
  }
  case object ColumnLtEqOpIcI extends ColumnOp2ICII {
    def op(a: I32Var, b: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      (for {
        a <- bufferIfNeededWithPreconditionI32(a.v)((segment: SegmentInt) =>
          segment.statistic.mightLtEq(b)
        )
      } yield I32Var(a match {
        case Right(a) => Left(a.elementwise_lteq(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })).logElapsed
    }
  }
  case object ColumnLtOpIcI extends ColumnOp2ICII {
    def op(a: I32Var, b: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      (for {
        a <- bufferIfNeededWithPreconditionI32(a.v)((segment: SegmentInt) =>
          segment.statistic.mightLt(b)
        )
      } yield I32Var(a match {
        case Right(a) => Left(a.elementwise_lt(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })).logElapsed
    }
  }
  case object ColumnGtEqOpII extends ColumnOp2III {
    def op(a: I32Var, b: I32Var)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededI32(a.v)
        b <- bufferIfNeededI32(b.v)
      } yield I32Var(Left(a.elementwise_gteq(b)))

    }.logElapsed
  }
  case object ColumnGtOpII extends ColumnOp2III {
    def op(a: I32Var, b: I32Var)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededI32(a.v)
        b <- bufferIfNeededI32(b.v)
      } yield I32Var(Left(a.elementwise_gt(b)))

    }.logElapsed
  }
  case object ColumnGtEqOpIcI extends ColumnOp2ICII {
    def op(a: I32Var, b: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededWithPreconditionI32(a.v)((segment: SegmentInt) =>
          segment.statistic.mightGtEq(b)
        )
      } yield I32Var(a match {
        case Right(a) => Left(a.elementwise_gteq(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }.logElapsed
  }
  case object ColumnGtOpIcI extends ColumnOp2ICII {
    def op(a: I32Var, b: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededWithPreconditionI32(a.v)((segment: SegmentInt) =>
          segment.statistic.mightGt(b)
        )
      } yield I32Var(a match {
        case Right(a) => Left(a.elementwise_gt(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }.logElapsed
  }
  case object ColumnEqOpStrcStr extends ColumnOp2StrCStrI {
    def op(a: StrVar, b: String)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededWithPreconditionString(a.v)(
          (segment: SegmentString) => segment.statistic.mightEq(b)
        )
      } yield I32Var(a match {
        case Right(a) => Left(a.elementwise_eq(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }.logElapsed
  }
  case object ColumnMatchesOpStrcStr extends ColumnOp2StrCStrI {
    def op(a: StrVar, b: String)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededString(a.v)
      } yield I32Var(Left(a.elementwise_matches(b)))
    }.logElapsed
  }
  case object ColumnContainedInOpStrcStrSet extends ColumnOp2StrCStrSetI {
    def op(a: StrVar, b: Set[String])(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededWithPreconditionString(a.v)(
          (segment: SegmentString) =>
            b.exists(b => segment.statistic.mightEq(b))
        )
      } yield I32Var(a match {
        case Right(a) => Left(a.elementwise_containedIn(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }.logElapsed
  }
  case object ColumnContainedInOpIcISet extends ColumnOp2ICISetI {
    def op(a: I32Var, b: Set[Int])(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededWithPreconditionI32(a.v)((segment: SegmentInt) =>
          b.exists(b => segment.statistic.mightEq(b))
        )
      } yield I32Var(a match {
        case Right(a) => Left(a.elementwise_containedIn(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }.logElapsed
  }
  case object ColumnContainedInOpDcDSet extends ColumnOp2DCDSetI {
    def op(a: F64Var, b: Set[Double])(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededWithPreconditionF64(a.v)((segment: SegmentDouble) =>
          b.exists(b => segment.statistic.mightEq(b))
        )
      } yield I32Var(a match {
        case Right(a) => Left(a.elementwise_containedIn(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }.logElapsed
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
    def op(a: F64Var, b: F64Var)(implicit
        tsc: TaskSystemComponents
    ): IO[F64Var] = {
      for {
        a <- bufferIfNeededF64(a.v)
        b <- bufferIfNeededF64(b.v)
      } yield F64Var(Left(a.elementwise_div(b)))

    }.logElapsed
  }
  case object ColumnMulOpDD extends ColumnOp2DDD {
    def op(a: F64Var, b: F64Var)(implicit
        tsc: TaskSystemComponents
    ): IO[F64Var] = {
      for {
        a <- bufferIfNeededF64(a.v)
        b <- bufferIfNeededF64(b.v)
      } yield F64Var(Left(a.elementwise_mul(b)))

    }.logElapsed
  }
  case object ColumnAddOpDD extends ColumnOp2DDD {
    def op(a: F64Var, b: F64Var)(implicit
        tsc: TaskSystemComponents
    ): IO[F64Var] = {
      for {
        a <- bufferIfNeededF64(a.v)
        b <- bufferIfNeededF64(b.v)
      } yield F64Var(Left(a.elementwise_add(b)))

    }.logElapsed
  }
  case object ColumnSubtractOpDD extends ColumnOp2DDD {
    def op(a: F64Var, b: F64Var)(implicit
        tsc: TaskSystemComponents
    ): IO[F64Var] = {
      for {
        a <- bufferIfNeededF64(a.v)
        b <- bufferIfNeededF64(b.v)
      } yield F64Var(Left(a.elementwise_subtract(b)))

    }.logElapsed
  }

  case object ColumnLtEqOpDD extends ColumnOp2DDI {
    def op(a: F64Var, b: F64Var)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededF64(a.v)
        b <- bufferIfNeededF64(b.v)
      } yield I32Var(Left(a.elementwise_lteq(b)))

    }.logElapsed
  }
  case object ColumnLtEqOpDcD extends ColumnOp2DcDI {
    def op(a: F64Var, b: Double)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededWithPreconditionF64(a.v)((segment: SegmentDouble) =>
          segment.statistic.mightLtEq(b)
        )
      } yield I32Var(a match {
        case Right(a) => Left(a.elementwise_lteq(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }.logElapsed

  }

  case object ColumnLtOpDD extends ColumnOp2DDI {
    def op(a: F64Var, b: F64Var)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededF64(a.v)
        b <- bufferIfNeededF64(b.v)
      } yield I32Var(Left(a.elementwise_lt(b)))

    }.logElapsed
  }
  case object ColumnLtOpDcD extends ColumnOp2DcDI {
    def op(a: F64Var, b: Double)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededWithPreconditionF64(a.v)((segment: SegmentDouble) =>
          segment.statistic.mightLt(b)
        )
      } yield I32Var(a match {
        case Right(a) => Left(a.elementwise_lt(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }.logElapsed
  }
  case object ColumnGtEqOpDD extends ColumnOp2DDI {
    def op(a: F64Var, b: F64Var)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededF64(a.v)
        b <- bufferIfNeededF64(b.v)
      } yield I32Var(Left(a.elementwise_gteq(b)))

    }.logElapsed
  }
  case object ColumnGtEqOpDcD extends ColumnOp2DcDI {
    def op(a: F64Var, b: Double)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededWithPreconditionF64(a.v)((segment: SegmentDouble) =>
          segment.statistic.mightGtEq(b)
        )
      } yield I32Var(a match {
        case Right(a) => Left(a.elementwise_gteq(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }.logElapsed
  }
  case object ColumnGtOpDD extends ColumnOp2DDI {
    def op(a: F64Var, b: F64Var)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededF64(a.v)
        b <- bufferIfNeededF64(b.v)
      } yield I32Var(Left(a.elementwise_gt(b)))

    }.logElapsed
  }
  case object ColumnGtOpDcD extends ColumnOp2DcDI {
    def op(a: F64Var, b: Double)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededWithPreconditionF64(a.v)((segment: SegmentDouble) =>
          segment.statistic.mightGt(b)
        )
      } yield I32Var(a match {
        case Right(a) => Left(a.elementwise_gt(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }.logElapsed
  }
  case object ColumnEqOpDD extends ColumnOp2DDI {
    def op(a: F64Var, b: F64Var)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededF64(a.v)
        b <- bufferIfNeededF64(b.v)
      } yield I32Var(Left(a.elementwise_eq(b)))

    }.logElapsed
  }
  case object ColumnEqOpDcD extends ColumnOp2DcDI {
    def op(a: F64Var, b: Double)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededWithPreconditionF64(a.v)((segment: SegmentDouble) =>
          segment.statistic.mightEq(b)
        )
      } yield I32Var(a match {
        case Right(a) => Left(a.elementwise_eq(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }.logElapsed
  }
  case object ColumnNEqOpDD extends ColumnOp2DDI {
    def op(a: F64Var, b: F64Var)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededF64(a.v)
        b <- bufferIfNeededF64(b.v)
      } yield I32Var(Left(a.elementwise_neq(b)))

    }.logElapsed
  }
  case object ColumnNEqOpDcD extends ColumnOp2DcDI {
    def op(a: F64Var, b: Double)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededF64(a.v)
      } yield I32Var(Left(a.elementwise_neq(b)))

    }.logElapsed
  }

  case object ColumnPrintfOpDcStr extends ColumnOp2DcStrStr {
    def op(a: F64Var, b: String)(implicit
        tsc: TaskSystemComponents
    ): IO[StrVar] = {
      for {
        a <- bufferIfNeededF64(a.v)
      } yield StrVar(Left(a.elementwise_printf(b)))

    }.logElapsed
  }
  case object ColumnPrintfOpIcStr extends ColumnOp2IcStrStr {
    def op(a: I32Var, b: String)(implicit
        tsc: TaskSystemComponents
    ): IO[StrVar] = {
      for {
        a <- bufferIfNeededI32(a.v)
      } yield StrVar(Left(a.elementwise_printf(b)))

    }.logElapsed
  }

  case object ColumnNEqOpII extends ColumnOp2III {
    def op(a: I32Var, b: I32Var)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededI32(a.v)
        b <- bufferIfNeededI32(b.v)
      } yield I32Var(Left(a.elementwise_neq(b)))

    }.logElapsed
  }
  case object ColumnNEqOpIcI extends ColumnOp2ICII {
    def op(a: I32Var, b: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededI32(a.v)
      } yield I32Var(Left(a.elementwise_neq(b)))

    }.logElapsed
  }

  case object ColumnPlusOpInstcL extends ColumnOp2InstcLInst {
    def op(a: InstVar, b: Long)(implicit
        tsc: TaskSystemComponents
    ): IO[InstVar] = {
      for {
        a <- bufferIfNeededInst(a.v)
      } yield InstVar(Left(a.elementwise_plus(b)))

    }.logElapsed
  }
  case object ColumnMinusOpInstcL extends ColumnOp2InstcLInst {
    def op(a: InstVar, b: Long)(implicit
        tsc: TaskSystemComponents
    ): IO[InstVar] = {
      for {
        a <- bufferIfNeededInst(a.v)
      } yield InstVar(Left(a.elementwise_minus(b)))

    }.logElapsed
  }

  case object ColumnLtEqOpInstInst extends ColumnOp2InstInstI {
    def op(a: InstVar, b: InstVar)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededInst(a.v)
        b <- bufferIfNeededInst(b.v)
      } yield I32Var(Left(a.elementwise_lteq(b)))

    }.logElapsed
  }

  case object ColumnLtEqOpInstcStr extends ColumnOp2InstcStrI {
    def op(a: InstVar, b: String)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededInst(a.v)
      } yield I32Var(
        Left(
          a.elementwise_lteq(java.time.Instant.parse(b).toEpochMilli())
        )
      )

    }.logElapsed
  }

  case object ColumnLtOpInstInst extends ColumnOp2InstInstI {
    def op(a: InstVar, b: InstVar)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededInst(a.v)
        b <- bufferIfNeededInst(b.v)
      } yield I32Var(Left(a.elementwise_lt(b)))

    }.logElapsed
  }

  case object ColumnLtOpInstcStr extends ColumnOp2InstcStrI {
    def op(a: InstVar, b: String)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededInst(a.v)
      } yield I32Var(
        Left(
          a.elementwise_lt(java.time.Instant.parse(b).toEpochMilli())
        )
      )

    }.logElapsed
  }
  case object ColumnGtEqOpInstInst extends ColumnOp2InstInstI {
    def op(a: InstVar, b: InstVar)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededInst(a.v)
        b <- bufferIfNeededInst(b.v)
      } yield I32Var(Left(a.elementwise_gteq(b)))

    }.logElapsed
  }

  case object ColumnGtEqOpInstcStr extends ColumnOp2InstcStrI {
    def op(a: InstVar, b: String)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededInst(a.v)
      } yield I32Var(
        Left(
          a.elementwise_gteq(java.time.Instant.parse(b).toEpochMilli())
        )
      )

    }.logElapsed
  }
  case object ColumnGtOpInstInst extends ColumnOp2InstInstI {
    def op(a: InstVar, b: InstVar)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededInst(a.v)
        b <- bufferIfNeededInst(b.v)
      } yield I32Var(Left(a.elementwise_gt(b)))

    }.logElapsed
  }

  case object ColumnGtOpInstcStr extends ColumnOp2InstcStrI {
    def op(a: InstVar, b: String)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededInst(a.v)
      } yield I32Var(
        Left(
          a.elementwise_gt(java.time.Instant.parse(b).toEpochMilli())
        )
      )

    }.logElapsed
  }
  case object ColumnEqOpInstInst extends ColumnOp2InstInstI {
    def op(a: InstVar, b: InstVar)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededInst(a.v)
        b <- bufferIfNeededInst(b.v)
      } yield I32Var(Left(a.elementwise_eq(b)))

    }.logElapsed
  }

  case object ColumnEqOpInstcStr extends ColumnOp2InstcStrI {
    def op(a: InstVar, b: String)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededInst(a.v)
      } yield I32Var(
        Left(a.elementwise_eq(java.time.Instant.parse(b).toEpochMilli()))
      )

    }.logElapsed
  }
  case object ColumnNEqOpInstInst extends ColumnOp2InstInstI {
    def op(a: InstVar, b: InstVar)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededInst(a.v)
        b <- bufferIfNeededInst(b.v)
      } yield I32Var(Left(a.elementwise_neq(b)))

    }.logElapsed
  }

  case object ColumnNEqOpInstcStr extends ColumnOp2InstcStrI {
    def op(a: InstVar, b: String)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededInst(a.v)
      } yield I32Var(
        Left(a.elementwise_neq(java.time.Instant.parse(b).toEpochMilli()))
      )

    }.logElapsed
  }

  case object ColumnPlusSecondsOpInstcInt extends ColumnOp2InstcIInst {
    def op(a: InstVar, b: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[InstVar] = {
      for {
        a <- bufferIfNeededInst(a.v)
      } yield InstVar(Left(a.elementwise_plus(b * 1000L)))

    }.logElapsed
  }
  case object ColumnMinusSecondsOpInstcInt extends ColumnOp2InstcIInst {
    def op(a: InstVar, b: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[InstVar] = {
      for {
        a <- bufferIfNeededInst(a.v)
      } yield InstVar(Left(a.elementwise_minus(b * 1000L)))

    }.logElapsed
  }
  case object ColumnPlusDaysOpInstcInt extends ColumnOp2InstcIInst {
    def op(a: InstVar, b: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[InstVar] = {
      for {
        a <- bufferIfNeededInst(a.v)
      } yield InstVar(Left(a.elementwise_plus(b * 1000L * 60L * 60L * 24L)))

    }.logElapsed
  }
  case object ColumnMinusDaysOpInstcInt extends ColumnOp2InstcIInst {
    def op(a: InstVar, b: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[InstVar] = {
      for {
        a <- bufferIfNeededInst(a.v)
      } yield InstVar(Left(a.elementwise_minus(b * 1000L * 60L * 60L * 24L)))

    }.logElapsed
  }

  case object ColumnLtEqOpInstcL extends ColumnOp2InstcLI {
    def op(a: InstVar, b: Long)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededInst(a.v)
      } yield I32Var(
        Left(
          a.elementwise_lteq(b)
        )
      )

    }.logElapsed
  }
  case object ColumnLtOpInstcL extends ColumnOp2InstcLI {
    def op(a: InstVar, b: Long)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededInst(a.v)
      } yield I32Var(
        Left(
          a.elementwise_lt(b)
        )
      )

    }.logElapsed
  }
  case object ColumnGtEqOpInstcL extends ColumnOp2InstcLI {
    def op(a: InstVar, b: Long)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededInst(a.v)
      } yield I32Var(
        Left(
          a.elementwise_gteq(b)
        )
      )

    }.logElapsed
  }
  case object ColumnGtOpInstcL extends ColumnOp2InstcLI {
    def op(a: InstVar, b: Long)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededInst(a.v)
      } yield I32Var(
        Left(
          a.elementwise_gt(b)
        )
      )

    }.logElapsed
  }
  case object ColumnEqOpInstcL extends ColumnOp2InstcLI {
    def op(a: InstVar, b: Long)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededInst(a.v)
      } yield I32Var(
        Left(
          a.elementwise_eq(b)
        )
      )

    }.logElapsed
  }
  case object ColumnNEqOpInstcL extends ColumnOp2InstcLI {
    def op(a: InstVar, b: Long)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededInst(a.v)
      } yield I32Var(
        Left(
          a.elementwise_neq(b)
        )
      )

    }.logElapsed
  }

  case object ColumnNEqOpStrcStr extends ColumnOp2StrCStrI {
    def op(a: StrVar, b: String)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededString(a.v)
      } yield I32Var(Left(a.elementwise_neq(b)))

    }.logElapsed
  }
  case object ColumnConcatOpStrcStr extends ColumnOp2StrCStrStr {
    def op(a: StrVar, b: String)(implicit
        tsc: TaskSystemComponents
    ): IO[StrVar] = {
      for {
        a <- bufferIfNeededString(a.v)
      } yield StrVar(Left(a.elementwise_concatenate(b)))

    }.logElapsed
  }
  case object ColumnConcatOpStrStr extends ColumnOp2StrStrStr {
    def op(a: StrVar, b: StrVar)(implicit
        tsc: TaskSystemComponents
    ): IO[StrVar] = {
      for {
        a <- bufferIfNeededString(a.v)
        b <- bufferIfNeededString(b.v)
      } yield StrVar(Left(a.elementwise_concatenate(b)))

    }.logElapsed
  }

  case object ColumnAndOpII extends ColumnOp2III {
    def op(a: I32Var, b: I32Var)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededI32(a.v)
        b <- bufferIfNeededI32(b.v)
      } yield I32Var(Left(a.elementwise_&&(b)))

    }.logElapsed
  }
  case object ColumnOrOpII extends ColumnOp2III {
    def op(a: I32Var, b: I32Var)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededI32(a.v)
        b <- bufferIfNeededI32(b.v)
      } yield I32Var(Left(a.`elementwise_||`(b)))

    }.logElapsed
  }

  case object ColumnPrintfOpLcStr extends ColumnOp2LCStrStr {
    def op(a: I64Var, b: String)(implicit
        tsc: TaskSystemComponents
    ): IO[StrVar] = {
      for {
        a <- bufferIfNeededI64(a.v)
      } yield StrVar(Left(a.elementwise_printf(b)))

    }.logElapsed
  }

  case object ColumnEqOpLL extends ColumnOp2LLI {
    def op(a: I64Var, b: I64Var)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededI64(a.v)
        b <- bufferIfNeededI64(b.v)
      } yield I32Var(Left(a.elementwise_eq(b)))

    }.logElapsed
  }
  case object ColumnEqOpLcL extends ColumnOp2LcLI {
    def op(a: I64Var, b: Long)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededWithPreconditionI64(a.v)((segment: SegmentLong) =>
          segment.statistic.mightEq(b)
        )
      } yield I32Var(a match {
        case Right(a) => Left(a.elementwise_eq(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }.logElapsed
  }

//

  case object ColumnLtEqOpStrStr extends ColumnOp2StrStrI {
    def op(a: StrVar, b: StrVar)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededString(a.v)
        b <- bufferIfNeededString(b.v)
      } yield I32Var(Left(a.elementwise_lteq(b)))

    }.logElapsed
  }
  case object ColumnLtEqOpStrcStr extends ColumnOp2StrCStrI {
    def op(a: StrVar, b: String)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededWithPreconditionString(a.v)(
          (segment: SegmentString) => segment.statistic.mightLtEq(b)
        )
      } yield I32Var(a match {
        case Right(a) => Left(a.elementwise_lteq(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }.logElapsed

  }
  case object ColumnGtEqOpStrStr extends ColumnOp2StrStrI {
    def op(a: StrVar, b: StrVar)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededString(a.v)
        b <- bufferIfNeededString(b.v)
      } yield I32Var(Left(a.elementwise_gteq(b)))

    }.logElapsed
  }
  case object ColumnGtEqOpStrcStr extends ColumnOp2StrCStrI {
    def op(a: StrVar, b: String)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededWithPreconditionString(a.v)(
          (segment: SegmentString) => segment.statistic.mightGtEq(b)
        )
      } yield I32Var(a match {
        case Right(a) => Left(a.elementwise_gteq(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }.logElapsed

  }
  case object ColumnLtOpStrStr extends ColumnOp2StrStrI {
    def op(a: StrVar, b: StrVar)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededString(a.v)
        b <- bufferIfNeededString(b.v)
      } yield I32Var(Left(a.elementwise_lt(b)))

    }.logElapsed
  }
  case object ColumnLtOpStrcStr extends ColumnOp2StrCStrI {
    def op(a: StrVar, b: String)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededWithPreconditionString(a.v)(
          (segment: SegmentString) => segment.statistic.mightLt(b)
        )
      } yield I32Var(a match {
        case Right(a) => Left(a.elementwise_lt(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }.logElapsed

  }
  case object ColumnGtOpStrStr extends ColumnOp2StrStrI {
    def op(a: StrVar, b: StrVar)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededString(a.v)
        b <- bufferIfNeededString(b.v)
      } yield I32Var(Left(a.elementwise_gt(b)))

    }.logElapsed
  }
  case object ColumnGtOpStrcStr extends ColumnOp2StrCStrI {
    def op(a: StrVar, b: String)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededWithPreconditionString(a.v)(
          (segment: SegmentString) => segment.statistic.mightGt(b)
        )
      } yield I32Var(a match {
        case Right(a) => Left(a.elementwise_gt(b))
        case Left(numEl) =>
          Left(BufferInt.constant(0, numEl))
      })
    }.logElapsed

  }

  case object ColumnEqOpStrStr extends ColumnOp2StrStrI {
    def op(a: StrVar, b: StrVar)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededString(a.v)
        b <- bufferIfNeededString(b.v)
      } yield I32Var(Left(a.elementwise_eq(b)))

    }.logElapsed
  }
  case object ColumnNEqOpStrStr extends ColumnOp2StrStrI {
    def op(a: StrVar, b: StrVar)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = {
      for {
        a <- bufferIfNeededString(a.v)
        b <- bufferIfNeededString(b.v)
      } yield I32Var(Left(a.elementwise_neq(b)))

    }.logElapsed
  }

}
