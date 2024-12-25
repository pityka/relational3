package ra3.lang.ops

import ra3.BufferInt
import ra3.lang.*
import ra3.*
import tasks.TaskSystemComponents
import cats.effect.*
import ra3.InstVar
import ra3.lang.util.*
sealed trait Op3 {
  type A0
  type A1
  type A2
  type T
  def op(a: A0, b: A1, c: A2)(implicit tsc: TaskSystemComponents): IO[T]
}

object Op3 {

  case object StringMatchAndReplaceOp extends Op3 {
    type A0 = StrVar
    type A1 = String
    type A2 = String
    type T = StrVar
    def op(a: StrVar, b: String, c: String)(implicit
        tsc: TaskSystemComponents
    ): IO[StrVar] =
      for {
        a <- bufferIfNeededString(a.v)
      } yield StrVar(Left(a.elementwise_matches_replace(b, c)))
  }

  sealed trait Op3III extends Op3 {
    type A0 = Int
    type A1 = Int
    type A2 = Int
    type T = Int
  }

  case object BufferSumGroupsOpII extends Op3 {
    type A0 = I32Var
    type A1 = BufferInt
    type A2 = Int
    type T = I32Var
    def op(a: I32Var, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] =
      for {
        a <- bufferIfNeededI32(a.v)
      } yield I32Var(Left(a.sumGroups(b, c)))
  }
  case object BufferSumGroupsOpDI extends Op3 {
    type A0 = F64Var
    type A1 = BufferInt
    type A2 = Int
    type T = F64Var
    def op(a: F64Var, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[F64Var] =
      for {
        a <- bufferIfNeededF64(a.v)
      } yield F64Var(Left(a.sumGroups(b, c)))
  }
  case object BufferCountGroupsOpDI extends Op3 {
    type A0 = F64Var
    type A1 = BufferInt
    type A2 = Int
    type T = F64Var
    def op(a: F64Var, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[F64Var] =
      for {
        a <- bufferIfNeededF64(a.v)
      } yield F64Var(Left(a.countGroups(b, c)))
  }
  case object BufferMeanGroupsOpDI extends Op3 {
    type A0 = F64Var
    type A1 = BufferInt
    type A2 = Int
    type T = F64Var
    def op(a: F64Var, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[F64Var] =
      for {
        a <- bufferIfNeededF64(a.v)
      } yield F64Var(Left(a.meanGroups(b, c)))
  }
  case object BufferFirstGroupsOpIIi extends Op3 {
    type A0 = I32Var
    type A1 = BufferInt
    type A2 = Int
    type T = I32Var
    def op(a: I32Var, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = for {
      a <- bufferIfNeededI32(a.v)
    } yield I32Var(Left(ColumnTag.I32.firstInGroup(a, b, c)))
  }
  case object BufferFirstGroupsOpDIi extends Op3 {
    type A0 = F64Var
    type A1 = BufferInt
    type A2 = Int
    type T = F64Var
    def op(a: F64Var, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[F64Var] = for {
      a <- bufferIfNeededF64(a.v)
    } yield F64Var(Left(a.firstInGroup(b, c)))
  }
  case object BufferFirstGroupsOpSIi extends Op3 {
    type A0 = StrVar
    type A1 = BufferInt
    type A2 = Int
    type T = StrVar
    def op(a: StrVar, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[StrVar] = for {
      a <- bufferIfNeededString(a.v)
    } yield StrVar(Left(a.firstInGroup(b, c)))
  }

  case object IfElseI32 extends Op3 {
    type A0 = I32Var
    type A1 = I32Var
    type A2 = I32Var
    type T = I32Var
    def op(a: I32Var, b: I32Var, c: I32Var)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = for {
      a <- bufferIfNeededI32(a.v)
      b <- bufferIfNeededI32(b.v)
      c <- bufferIfNeededI32(c.v)
    } yield I32Var(Left(a.elementwise_choose(b, c)))
  }
  case object IfElseCI32 extends Op3 {
    type A0 = I32Var
    type A1 = Int
    type A2 = I32Var
    type T = I32Var
    def op(a: I32Var, b: Int, c: I32Var)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = for {
      a <- bufferIfNeededI32(a.v)
      c <- bufferIfNeededI32(c.v)
    } yield I32Var(Left(a.elementwise_choose(b, c)))
  }
  case object IfElseI32C extends Op3 {
    type A0 = I32Var
    type A1 = I32Var
    type A2 = Int
    type T = I32Var
    def op(a: I32Var, b: I32Var, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = for {
      a <- bufferIfNeededI32(a.v)
      b <- bufferIfNeededI32(b.v)
    } yield I32Var(Left(a.elementwise_choose(b, c)))
  }
  case object IfElseI32CC extends Op3 {
    type A0 = I32Var
    type A1 = Int
    type A2 = Int
    type T = I32Var
    def op(a: I32Var, b: Int, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = for {
      a <- bufferIfNeededI32(a.v)
    } yield I32Var(Left(a.elementwise_choose(b, c)))
  }
  case object IfElseI64 extends Op3 {
    type A0 = I32Var
    type A1 = I64Var
    type A2 = I64Var
    type T = I64Var
    def op(a: I32Var, b: I64Var, c: I64Var)(implicit
        tsc: TaskSystemComponents
    ): IO[I64Var] = for {
      a <- bufferIfNeededI32(a.v)
      b <- bufferIfNeededI64(b.v)
      c <- bufferIfNeededI64(c.v)
    } yield I64Var(Left(a.elementwise_choose(b, c)))
  }
  case object IfElseCI64 extends Op3 {
    type A0 = I32Var
    type A1 = Long
    type A2 = I64Var
    type T = I64Var
    def op(a: I32Var, b: Long, c: I64Var)(implicit
        tsc: TaskSystemComponents
    ): IO[I64Var] = for {
      a <- bufferIfNeededI32(a.v)
      c <- bufferIfNeededI64(c.v)
    } yield I64Var(Left(a.elementwise_choose(b, c)))
  }
  case object IfElseI64C extends Op3 {
    type A0 = I32Var
    type A1 = I64Var
    type A2 = Long
    type T = I64Var
    def op(a: I32Var, b: I64Var, c: Long)(implicit
        tsc: TaskSystemComponents
    ): IO[I64Var] = for {
      a <- bufferIfNeededI32(a.v)
      b <- bufferIfNeededI64(b.v)
    } yield I64Var(Left(a.elementwise_choose(b, c)))
  }
  case object IfElseI64CC extends Op3 {
    type A0 = I32Var
    type A1 = Long
    type A2 = Long
    type T = I64Var
    def op(a: I32Var, b: Long, c: Long)(implicit
        tsc: TaskSystemComponents
    ): IO[I64Var] = for {
      a <- bufferIfNeededI32(a.v)
    } yield I64Var(Left(a.elementwise_choose(b, c)))
  }

  case object IfElseF64 extends Op3 {
    type A0 = I32Var
    type A1 = F64Var
    type A2 = F64Var
    type T = F64Var
    def op(a: I32Var, b: F64Var, c: F64Var)(implicit
        tsc: TaskSystemComponents
    ): IO[F64Var] = for {
      a <- bufferIfNeededI32(a.v)
      b <- bufferIfNeededF64(b.v)
      c <- bufferIfNeededF64(c.v)
    } yield F64Var(Left(a.elementwise_choose(b, c)))
  }

  case object IfElseCF64 extends Op3 {
    type A0 = I32Var
    type A1 = Double
    type A2 = F64Var
    type T = F64Var
    def op(a: I32Var, b: Double, c: F64Var)(implicit
        tsc: TaskSystemComponents
    ): IO[F64Var] = for {
      a <- bufferIfNeededI32(a.v)
      c <- bufferIfNeededF64(c.v)
    } yield F64Var(Left(a.elementwise_choose(b, c)))
  }

  case object IfElseF64C extends Op3 {
    type A0 = I32Var
    type A1 = F64Var
    type A2 = Double
    type T = F64Var
    def op(a: I32Var, b: F64Var, c: Double)(implicit
        tsc: TaskSystemComponents
    ): IO[F64Var] = for {
      a <- bufferIfNeededI32(a.v)
      b <- bufferIfNeededF64(b.v)
    } yield F64Var(Left(a.elementwise_choose(b, c)))
  }

  case object IfElseF64CC extends Op3 {
    type A0 = I32Var
    type A1 = Double
    type A2 = Double
    type T = F64Var
    def op(a: I32Var, b: Double, c: Double)(implicit
        tsc: TaskSystemComponents
    ): IO[F64Var] = for {
      a <- bufferIfNeededI32(a.v)
    } yield F64Var(Left(a.elementwise_choose(b, c)))
  }

  case object IfElseInst extends Op3 {
    type A0 = I32Var
    type A1 = InstVar
    type A2 = InstVar
    type T = InstVar
    def op(a: I32Var, b: InstVar, c: InstVar)(implicit
        tsc: TaskSystemComponents
    ): IO[InstVar] = for {
      a <- bufferIfNeededI32(a.v)
      b <- bufferIfNeededInst(b.v)
      c <- bufferIfNeededInst(c.v)
    } yield InstVar(Left(a.elementwise_choose(b, c)))
  }

  case object IfElseCInst extends Op3 {
    type A0 = I32Var
    type A1 = Long
    type A2 = InstVar
    type T = InstVar
    def op(a: I32Var, b: Long, c: InstVar)(implicit
        tsc: TaskSystemComponents
    ): IO[InstVar] = for {
      a <- bufferIfNeededI32(a.v)
      c <- bufferIfNeededInst(c.v)
    } yield InstVar(Left(a.elementwise_choose(b, c)))
  }

  case object IfElseInstC extends Op3 {
    type A0 = I32Var
    type A1 = InstVar
    type A2 = Long
    type T = InstVar
    def op(a: I32Var, b: InstVar, c: Long)(implicit
        tsc: TaskSystemComponents
    ): IO[InstVar] = for {
      a <- bufferIfNeededI32(a.v)
      b <- bufferIfNeededInst(b.v)
    } yield InstVar(Left(a.elementwise_choose(b, c)))
  }

  case object IfElseInstCC extends Op3 {
    type A0 = I32Var
    type A1 = Long
    type A2 = Long
    type T = InstVar
    def op(a: I32Var, b: Long, c: Long)(implicit
        tsc: TaskSystemComponents
    ): IO[InstVar] = for {
      a <- bufferIfNeededI32(a.v)
    } yield InstVar(Left(a.elementwise_choose_inst(b, c)))
  }

  case object IfElseStr extends Op3 {
    type A0 = I32Var
    type A1 = StrVar
    type A2 = StrVar
    type T = StrVar
    def op(a: I32Var, b: StrVar, c: StrVar)(implicit
        tsc: TaskSystemComponents
    ): IO[StrVar] = for {
      a <- bufferIfNeededI32(a.v)
      b <- bufferIfNeededString(b.v)
      c <- bufferIfNeededString(c.v)
    } yield StrVar(Left(a.elementwise_choose(b, c)))
  }

  case object IfElseCStr extends Op3 {
    type A0 = I32Var
    type A1 = String
    type A2 = StrVar
    type T = StrVar
    def op(a: I32Var, b: String, c: StrVar)(implicit
        tsc: TaskSystemComponents
    ): IO[StrVar] = for {
      a <- bufferIfNeededI32(a.v)
      c <- bufferIfNeededString(c.v)
    } yield StrVar(Left(a.elementwise_choose(b, c)))
  }

  case object IfElseStrC extends Op3 {
    type A0 = I32Var
    type A1 = StrVar
    type A2 = String
    type T = StrVar
    def op(a: I32Var, b: StrVar, c: String)(implicit
        tsc: TaskSystemComponents
    ): IO[StrVar] = for {
      a <- bufferIfNeededI32(a.v)
      b <- bufferIfNeededString(b.v)
    } yield StrVar(Left(a.elementwise_choose(b, c)))
  }

  case object IfElseStrCC extends Op3 {
    type A0 = I32Var
    type A1 = String
    type A2 = String
    type T = StrVar
    def op(a: I32Var, b: String, c: String)(implicit
        tsc: TaskSystemComponents
    ): IO[StrVar] = for {
      a <- bufferIfNeededI32(a.v)
    } yield StrVar(Left(a.elementwise_choose(b, c)))
  }

  case object BufferMinGroupsOpD extends Op3 {
    type A0 = F64Var
    type A1 = BufferInt
    type A2 = Int
    type T = F64Var
    def op(a: F64Var, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[F64Var] = for {
      a <- bufferIfNeededF64(a.v)
    } yield F64Var(Left(a.minInGroups(b, c)))
  }
  case object BufferMaxGroupsOpD extends Op3 {
    type A0 = F64Var
    type A1 = BufferInt
    type A2 = Int
    type T = F64Var
    def op(a: F64Var, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[F64Var] = for {
      a <- bufferIfNeededF64(a.v)
    } yield F64Var(Left(a.maxInGroups(b, c)))
  }
  case object BufferHasMissingInGroupsOpD extends Op3 {
    type A0 = F64Var
    type A1 = BufferInt
    type A2 = Int
    type T = I32Var
    def op(a: F64Var, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = for {
      a <- bufferIfNeededF64(a.v)
    } yield I32Var(Left(a.hasMissingInGroup(b, c)))
  }

  case object BufferCountDistinctInGroupsOpD extends Op3 {
    type A0 = F64Var
    type A1 = BufferInt
    type A2 = Int
    type T = I32Var
    def op(a: F64Var, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = for {
      a <- bufferIfNeededF64(a.v)
    } yield I32Var(Left(a.countDistinctGroups(b, c)))
  }

  //

  case object BufferMinGroupsOpI extends Op3 {
    type A0 = I32Var
    type A1 = BufferInt
    type A2 = Int
    type T = I32Var
    def op(a: I32Var, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = for {
      a <- bufferIfNeededI32(a.v)
    } yield I32Var(Left(a.minInGroups(b, c)))
  }
  case object BufferMaxGroupsOpI extends Op3 {
    type A0 = I32Var
    type A1 = BufferInt
    type A2 = Int
    type T = I32Var
    def op(a: I32Var, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = for {
      a <- bufferIfNeededI32(a.v)
    } yield I32Var(Left(a.maxInGroups(b, c)))
  }
  case object BufferHasMissingInGroupsOpI extends Op3 {
    type A0 = I32Var
    type A1 = BufferInt
    type A2 = Int
    type T = I32Var
    def op(a: I32Var, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = for {
      a <- bufferIfNeededI32(a.v)
    } yield I32Var(Left(a.hasMissingInGroup(b, c)))
  }
  case object BufferCountInGroupsOpI extends Op3 {
    type A0 = I32Var
    type A1 = BufferInt
    type A2 = Int
    type T = I32Var
    def op(a: I32Var, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = for {
      a <- bufferIfNeededI32(a.v)
    } yield I32Var(Left(a.countInGroups(b, c)))
  }
  case object BufferCountDistinctInGroupsOpI extends Op3 {
    type A0 = I32Var
    type A1 = BufferInt
    type A2 = Int
    type T = I32Var
    def op(a: I32Var, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = for {
      a <- bufferIfNeededI32(a.v)
    } yield I32Var(Left(a.countDistinctInGroups(b, c)))
  }
  case object BufferAllInGroupsOpI extends Op3 {
    type A0 = I32Var
    type A1 = BufferInt
    type A2 = Int
    type T = I32Var
    def op(a: I32Var, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = for {
      a <- bufferIfNeededI32(a.v)
    } yield I32Var(Left(a.allInGroups(b, c)))
  }
  case object BufferAnyInGroupsOpI extends Op3 {
    type A0 = I32Var
    type A1 = BufferInt
    type A2 = Int
    type T = I32Var
    def op(a: I32Var, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = for {
      a <- bufferIfNeededI32(a.v)
    } yield I32Var(Left(a.anyInGroups(b, c)))
  }
  case object BufferNoneInGroupsOpI extends Op3 {
    type A0 = I32Var
    type A1 = BufferInt
    type A2 = Int
    type T = I32Var
    def op(a: I32Var, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = for {
      a <- bufferIfNeededI32(a.v)
    } yield I32Var(Left(a.noneInGroups(b, c)))
  }

  //

  case object BufferHasMissingInGroupsOpInst extends Op3 {
    type A0 = InstVar
    type A1 = BufferInt
    type A2 = Int
    type T = I32Var
    def op(a: InstVar, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = for {
      a <- bufferIfNeededInst(a.v)
    } yield I32Var(Left(a.hasMissingInGroup(b, c)))
  }
  case object BufferCountInGroupsOpInst extends Op3 {
    type A0 = InstVar
    type A1 = BufferInt
    type A2 = Int
    type T = I32Var
    def op(a: InstVar, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = for {
      a <- bufferIfNeededInst(a.v)
    } yield I32Var(Left(a.countInGroups(b, c)))
  }
  case object BufferCountDistinctInGroupsOpInst extends Op3 {
    type A0 = InstVar
    type A1 = BufferInt
    type A2 = Int
    type T = I32Var
    def op(a: InstVar, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = for {
      a <- bufferIfNeededInst(a.v)
    } yield I32Var(Left(a.countDistinctInGroups(b, c)))
  }

  case object BufferMinGroupsOpInst extends Op3 {
    type A0 = InstVar
    type A1 = BufferInt
    type A2 = Int
    type T = InstVar
    def op(a: InstVar, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[InstVar] = for {
      a <- bufferIfNeededInst(a.v)
    } yield InstVar(Left(a.minInGroups(b, c)))
  }
  case object BufferMaxGroupsOpInst extends Op3 {
    type A0 = InstVar
    type A1 = BufferInt
    type A2 = Int
    type T = InstVar
    def op(a: InstVar, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[InstVar] = for {
      a <- bufferIfNeededInst(a.v)
    } yield InstVar(Left(a.maxInGroups(b, c)))
  }

  //

  case object BufferHasMissingInGroupsOpS extends Op3 {
    type A0 = StrVar
    type A1 = BufferInt
    type A2 = Int
    type T = I32Var
    def op(a: StrVar, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = for {
      a <- bufferIfNeededString(a.v)
    } yield I32Var(Left(a.hasMissingInGroup(b, c)))
  }
  case object BufferCountInGroupsOpS extends Op3 {
    type A0 = StrVar
    type A1 = BufferInt
    type A2 = Int
    type T = I32Var
    def op(a: StrVar, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = for {
      a <- bufferIfNeededString(a.v)
    } yield I32Var(Left(a.countInGroups(b, c)))
  }
  case object BufferCountDistinctInGroupsOpS extends Op3 {
    type A0 = StrVar
    type A1 = BufferInt
    type A2 = Int
    type T = I32Var
    def op(a: StrVar, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = for {
      a <- bufferIfNeededString(a.v)
    } yield I32Var(Left(a.countDistinctInGroups(b, c)))
  }

  case object BufferSubstringOpS extends Op3 {
    type A0 = StrVar
    type A1 = Int
    type A2 = Int
    type T = StrVar
    def op(a: StrVar, start: Int, len: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[StrVar] = for {
      a <- bufferIfNeededString(a.v)
    } yield StrVar(Left(a.elementwise_substring(start, len)))
  }

  case object BufferFirstGroupsOpInst extends Op3 {
    type A0 = InstVar
    type A1 = BufferInt
    type A2 = Int
    type T = InstVar
    def op(a: InstVar, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[InstVar] = for {
      a <- bufferIfNeededInst(a.v)
    } yield InstVar(Left(a.firstInGroup(b, c)))
  }

  case object BufferCountInGroupsOpL extends Op3 {
    type A0 = I64Var
    type A1 = BufferInt
    type A2 = Int
    type T = I32Var
    def op(a: I64Var, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = for {
      a <- bufferIfNeededI64(a.v)
    } yield I32Var(Left(a.countInGroups(b, c)))
  }
  case object BufferCountDistinctInGroupsOpL extends Op3 {
    type A0 = I64Var
    type A1 = BufferInt
    type A2 = Int
    type T = I32Var
    def op(a: I64Var, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = for {
      a <- bufferIfNeededI64(a.v)
    } yield I32Var(Left(a.countDistinctInGroups(b, c)))
  }

  case object BufferFirstGroupsOpL extends Op3 {
    type A0 = I64Var
    type A1 = BufferInt
    type A2 = Int
    type T = I64Var
    def op(a: I64Var, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I64Var] = for {
      a <- bufferIfNeededI64(a.v)
    } yield I64Var(Left(a.firstInGroup(b, c)))
  }

  case object BufferHasMissingInGroupsOpL extends Op3 {
    type A0 = I64Var
    type A1 = BufferInt
    type A2 = Int
    type T = I32Var
    def op(a: I64Var, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[I32Var] = for {
      a <- bufferIfNeededI64(a.v)
    } yield I32Var(Left(a.hasMissingInGroup(b, c)))
  }

}
