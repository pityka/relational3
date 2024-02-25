package ra3.lang.ops

import ra3.BufferInt
import ra3.lang._
import ra3._
import tasks.TaskSystemComponents
import cats.effect._
import ra3.DInst
private[ra3] sealed trait Op3 {
  type A0
  type A1
  type A2
  type T
  def op(a: A0, b: A1, c: A2)(implicit tsc: TaskSystemComponents): IO[T]
}

private[ra3] object Op3 {

  case object StringMatchAndReplaceOp extends Op3 {
    type A0 = DStr
    type A1 = String
    type A2 = String
    type T = DStr
    def op(a: DStr, b: String, c: String)(implicit
        tsc: TaskSystemComponents
    ): IO[DStr] =
      for {
        a <- bufferIfNeeded(a)
      } yield Left(a.elementwise_matches_replace(b, c))
  }

  sealed trait Op3III extends Op3 {
    type A0 = Int
    type A1 = Int
    type A2 = Int
    type T = Int
  }

  case object BufferSumGroupsOpII extends Op3 {
    type A0 = DI32
    type A1 = BufferInt
    type A2 = Int
    type T = DI32
    def op(a: DI32, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] =
      for {
        a <- bufferIfNeeded(a)
      } yield Left(a.sumGroups(b, c))
  }
  case object BufferSumGroupsOpDI extends Op3 {
    type A0 = DF64
    type A1 = BufferInt
    type A2 = Int
    type T = DF64
    def op(a: DF64, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DF64] =
      for {
        a <- bufferIfNeeded(a)
      } yield Left(a.sumGroups(b, c))
  }
  case object BufferCountGroupsOpDI extends Op3 {
    type A0 = DF64
    type A1 = BufferInt
    type A2 = Int
    type T = DF64
    def op(a: DF64, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DF64] =
      for {
        a <- bufferIfNeeded(a)
      } yield Left(a.countGroups(b, c))
  }
  case object BufferMeanGroupsOpDI extends Op3 {
    type A0 = DF64
    type A1 = BufferInt
    type A2 = Int
    type T = DF64
    def op(a: DF64, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DF64] =
      for {
        a <- bufferIfNeeded(a)
      } yield Left(a.meanGroups(b, c))
  }
  case object BufferFirstGroupsOpIIi extends Op3 {
    type A0 = DI32
    type A1 = BufferInt
    type A2 = Int
    type T = DI32
    def op(a: DI32, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.firstInGroup(b, c))
  }
  case object BufferFirstGroupsOpDIi extends Op3 {
    type A0 = DF64
    type A1 = BufferInt
    type A2 = Int
    type T = DF64
    def op(a: DF64, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DF64] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.firstInGroup(b, c))
  }
  case object BufferFirstGroupsOpSIi extends Op3 {
    type A0 = DStr
    type A1 = BufferInt
    type A2 = Int
    type T = DStr
    def op(a: DStr, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DStr] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.firstInGroup(b, c))
  }

  case object IfElseI32 extends Op3 {
    type A0 = DI32
    type A1 = DI32
    type A2 = DI32
    type T = DI32
    def op(a: DI32, b: DI32, c: DI32)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = for {
      a <- bufferIfNeeded(a)
      b <- bufferIfNeeded(b)
      c <- bufferIfNeeded(c)
    } yield Left(a.elementwise_choose(b, c))
  }
  case object IfElseCI32 extends Op3 {
    type A0 = DI32
    type A1 = Int
    type A2 = DI32
    type T = DI32
    def op(a: DI32, b: Int, c: DI32)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = for {
      a <- bufferIfNeeded(a)
      c <- bufferIfNeeded(c)
    } yield Left(a.elementwise_choose(b, c))
  }
  case object IfElseI32C extends Op3 {
    type A0 = DI32
    type A1 = DI32
    type A2 = Int
    type T = DI32
    def op(a: DI32, b: DI32, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = for {
      a <- bufferIfNeeded(a)
      b <- bufferIfNeeded(b)
    } yield Left(a.elementwise_choose(b, c))
  }
  case object IfElseI32CC extends Op3 {
    type A0 = DI32
    type A1 = Int
    type A2 = Int
    type T = DI32
    def op(a: DI32, b: Int, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.elementwise_choose(b, c))
  }
  case object IfElseI64 extends Op3 {
    type A0 = DI32
    type A1 = DI64
    type A2 = DI64
    type T = DI64
    def op(a: DI32, b: DI64, c: DI64)(implicit
        tsc: TaskSystemComponents
    ): IO[DI64] = for {
      a <- bufferIfNeeded(a)
      b <- bufferIfNeeded(b)
      c <- bufferIfNeeded(c)
    } yield Left(a.elementwise_choose(b, c))
  }
  case object IfElseCI64 extends Op3 {
    type A0 = DI32
    type A1 = Long
    type A2 = DI64
    type T = DI64
    def op(a: DI32, b: Long, c: DI64)(implicit
        tsc: TaskSystemComponents
    ): IO[DI64] = for {
      a <- bufferIfNeeded(a)
      c <- bufferIfNeeded(c)
    } yield Left(a.elementwise_choose(b, c))
  }
  case object IfElseI64C extends Op3 {
    type A0 = DI32
    type A1 = DI64
    type A2 = Long
    type T = DI64
    def op(a: DI32, b: DI64, c: Long)(implicit
        tsc: TaskSystemComponents
    ): IO[DI64] = for {
      a <- bufferIfNeeded(a)
      b <- bufferIfNeeded(b)
    } yield Left(a.elementwise_choose(b, c))
  }
  case object IfElseI64CC extends Op3 {
    type A0 = DI32
    type A1 = Long
    type A2 = Long
    type T = DI64
    def op(a: DI32, b: Long, c: Long)(implicit
        tsc: TaskSystemComponents
    ): IO[DI64] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.elementwise_choose(b, c))
  }

  case object IfElseF64 extends Op3 {
    type A0 = DI32
    type A1 = DF64
    type A2 = DF64
    type T = DF64
    def op(a: DI32, b: DF64, c: DF64)(implicit
        tsc: TaskSystemComponents
    ): IO[DF64] = for {
      a <- bufferIfNeeded(a)
      b <- bufferIfNeeded(b)
      c <- bufferIfNeeded(c)
    } yield Left(a.elementwise_choose(b, c))
  }

  case object IfElseCF64 extends Op3 {
    type A0 = DI32
    type A1 = Double
    type A2 = DF64
    type T = DF64
    def op(a: DI32, b: Double, c: DF64)(implicit
        tsc: TaskSystemComponents
    ): IO[DF64] = for {
      a <- bufferIfNeeded(a)
      c <- bufferIfNeeded(c)
    } yield Left(a.elementwise_choose(b, c))
  }

  case object IfElseF64C extends Op3 {
    type A0 = DI32
    type A1 = DF64
    type A2 = Double
    type T = DF64
    def op(a: DI32, b: DF64, c: Double)(implicit
        tsc: TaskSystemComponents
    ): IO[DF64] = for {
      a <- bufferIfNeeded(a)
      b <- bufferIfNeeded(b)
    } yield Left(a.elementwise_choose(b, c))
  }

  case object IfElseF64CC extends Op3 {
    type A0 = DI32
    type A1 = Double
    type A2 = Double
    type T = DF64
    def op(a: DI32, b: Double, c: Double)(implicit
        tsc: TaskSystemComponents
    ): IO[DF64] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.elementwise_choose(b, c))
  }

  case object IfElseInst extends Op3 {
    type A0 = DI32
    type A1 = DInst
    type A2 = DInst
    type T = DInst
    def op(a: DI32, b: DInst, c: DInst)(implicit
        tsc: TaskSystemComponents
    ): IO[DInst] = for {
      a <- bufferIfNeeded(a)
      b <- bufferIfNeeded(b)
      c <- bufferIfNeeded(c)
    } yield Left(a.elementwise_choose(b, c))
  }

  case object IfElseCInst extends Op3 {
    type A0 = DI32
    type A1 = Long
    type A2 = DInst
    type T = DInst
    def op(a: DI32, b: Long, c: DInst)(implicit
        tsc: TaskSystemComponents
    ): IO[DInst] = for {
      a <- bufferIfNeeded(a)
      c <- bufferIfNeeded(c)
    } yield Left(a.elementwise_choose(b, c))
  }

  case object IfElseInstC extends Op3 {
    type A0 = DI32
    type A1 = DInst
    type A2 = Long
    type T = DInst
    def op(a: DI32, b: DInst, c: Long)(implicit
        tsc: TaskSystemComponents
    ): IO[DInst] = for {
      a <- bufferIfNeeded(a)
      b <- bufferIfNeeded(b)
    } yield Left(a.elementwise_choose(b, c))
  }

  case object IfElseInstCC extends Op3 {
    type A0 = DI32
    type A1 = Long
    type A2 = Long
    type T = DInst
    def op(a: DI32, b: Long, c: Long)(implicit
        tsc: TaskSystemComponents
    ): IO[DInst] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.elementwise_choose_inst(b, c))
  }

  case object IfElseStr extends Op3 {
    type A0 = DI32
    type A1 = DStr
    type A2 = DStr
    type T = DStr
    def op(a: DI32, b: DStr, c: DStr)(implicit
        tsc: TaskSystemComponents
    ): IO[DStr] = for {
      a <- bufferIfNeeded(a)
      b <- bufferIfNeeded(b)
      c <- bufferIfNeeded(c)
    } yield Left(a.elementwise_choose(b, c))
  }

  case object IfElseCStr extends Op3 {
    type A0 = DI32
    type A1 = String
    type A2 = DStr
    type T = DStr
    def op(a: DI32, b: String, c: DStr)(implicit
        tsc: TaskSystemComponents
    ): IO[DStr] = for {
      a <- bufferIfNeeded(a)
      c <- bufferIfNeeded(c)
    } yield Left(a.elementwise_choose(b, c))
  }

  case object IfElseStrC extends Op3 {
    type A0 = DI32
    type A1 = DStr
    type A2 = String
    type T = DStr
    def op(a: DI32, b: DStr, c: String)(implicit
        tsc: TaskSystemComponents
    ): IO[DStr] = for {
      a <- bufferIfNeeded(a)
      b <- bufferIfNeeded(b)
    } yield Left(a.elementwise_choose(b, c))
  }

  case object IfElseStrCC extends Op3 {
    type A0 = DI32
    type A1 = String
    type A2 = String
    type T = DStr
    def op(a: DI32, b: String, c: String)(implicit
        tsc: TaskSystemComponents
    ): IO[DStr] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.elementwise_choose(b, c))
  }

  case object BufferMinGroupsOpD extends Op3 {
    type A0 = DF64
    type A1 = BufferInt
    type A2 = Int
    type T = DF64
    def op(a: DF64, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DF64] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.minInGroups(b, c))
  }
  case object BufferMaxGroupsOpD extends Op3 {
    type A0 = DF64
    type A1 = BufferInt
    type A2 = Int
    type T = DF64
    def op(a: DF64, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DF64] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.maxInGroups(b, c))
  }
  case object BufferHasMissingInGroupsOpD extends Op3 {
    type A0 = DF64
    type A1 = BufferInt
    type A2 = Int
    type T = DI32
    def op(a: DF64, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.hasMissingInGroup(b, c))
  }

  case object BufferCountDistinctInGroupsOpD extends Op3 {
    type A0 = DF64
    type A1 = BufferInt
    type A2 = Int
    type T = DI32
    def op(a: DF64, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.countDistinctGroups(b, c))
  }

  //

  case object BufferMinGroupsOpI extends Op3 {
    type A0 = DI32
    type A1 = BufferInt
    type A2 = Int
    type T = DI32
    def op(a: DI32, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.minInGroups(b, c))
  }
  case object BufferMaxGroupsOpI extends Op3 {
    type A0 = DI32
    type A1 = BufferInt
    type A2 = Int
    type T = DI32
    def op(a: DI32, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.maxInGroups(b, c))
  }
  case object BufferHasMissingInGroupsOpI extends Op3 {
    type A0 = DI32
    type A1 = BufferInt
    type A2 = Int
    type T = DI32
    def op(a: DI32, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.hasMissingInGroup(b, c))
  }
  case object BufferCountInGroupsOpI extends Op3 {
    type A0 = DI32
    type A1 = BufferInt
    type A2 = Int
    type T = DI32
    def op(a: DI32, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.countInGroups(b, c))
  }
  case object BufferCountDistinctInGroupsOpI extends Op3 {
    type A0 = DI32
    type A1 = BufferInt
    type A2 = Int
    type T = DI32
    def op(a: DI32, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.countDistinctInGroups(b, c))
  }
  case object BufferAllInGroupsOpI extends Op3 {
    type A0 = DI32
    type A1 = BufferInt
    type A2 = Int
    type T = DI32
    def op(a: DI32, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.allInGroups(b, c))
  }
  case object BufferAnyInGroupsOpI extends Op3 {
    type A0 = DI32
    type A1 = BufferInt
    type A2 = Int
    type T = DI32
    def op(a: DI32, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.anyInGroups(b, c))
  }
  case object BufferNoneInGroupsOpI extends Op3 {
    type A0 = DI32
    type A1 = BufferInt
    type A2 = Int
    type T = DI32
    def op(a: DI32, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.noneInGroups(b, c))
  }

  //

  case object BufferHasMissingInGroupsOpInst extends Op3 {
    type A0 = DInst
    type A1 = BufferInt
    type A2 = Int
    type T = DI32
    def op(a: DInst, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.hasMissingInGroup(b, c))
  }
  case object BufferCountInGroupsOpInst extends Op3 {
    type A0 = DInst
    type A1 = BufferInt
    type A2 = Int
    type T = DI32
    def op(a: DInst, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.countInGroups(b, c))
  }
  case object BufferCountDistinctInGroupsOpInst extends Op3 {
    type A0 = DInst
    type A1 = BufferInt
    type A2 = Int
    type T = DI32
    def op(a: DInst, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.countDistinctInGroups(b, c))
  }

  case object BufferMinGroupsOpInst extends Op3 {
    type A0 = DInst
    type A1 = BufferInt
    type A2 = Int
    type T = DInst
    def op(a: DInst, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DInst] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.minInGroups(b, c))
  }
  case object BufferMaxGroupsOpInst extends Op3 {
    type A0 = DInst
    type A1 = BufferInt
    type A2 = Int
    type T = DInst
    def op(a: DInst, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DInst] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.maxInGroups(b, c))
  }

  //

  case object BufferHasMissingInGroupsOpS extends Op3 {
    type A0 = DStr
    type A1 = BufferInt
    type A2 = Int
    type T = DI32
    def op(a: DStr, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.hasMissingInGroup(b, c))
  }
  case object BufferCountInGroupsOpS extends Op3 {
    type A0 = DStr
    type A1 = BufferInt
    type A2 = Int
    type T = DI32
    def op(a: DStr, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.countInGroups(b, c))
  }
  case object BufferCountDistinctInGroupsOpS extends Op3 {
    type A0 = DStr
    type A1 = BufferInt
    type A2 = Int
    type T = DI32
    def op(a: DStr, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.countDistinctInGroups(b, c))
  }

  case object BufferSubstringOpS extends Op3 {
    type A0 = DStr
    type A1 = Int
    type A2 = Int
    type T = DStr
    def op(a: DStr, start: Int, len: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DStr] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.elementwise_substring(start, len))
  }

  case object BufferFirstGroupsOpInst extends Op3 {
    type A0 = DInst
    type A1 = BufferInt
    type A2 = Int
    type T = DInst
    def op(a: DInst, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DInst] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.firstInGroup(b, c))
  }

  case object BufferCountInGroupsOpL extends Op3 {
    type A0 = DI64
    type A1 = BufferInt
    type A2 = Int
    type T = DI32
    def op(a: DI64, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.countInGroups(b, c))
  }
  case object BufferCountDistinctInGroupsOpL extends Op3 {
    type A0 = DI64
    type A1 = BufferInt
    type A2 = Int
    type T = DI32
    def op(a: DI64, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.countDistinctInGroups(b, c))
  }

  case object BufferFirstGroupsOpL extends Op3 {
    type A0 = DI64
    type A1 = BufferInt
    type A2 = Int
    type T = DI64
    def op(a: DI64, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DI64] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.firstInGroup(b, c))
  }

  case object BufferHasMissingInGroupsOpL extends Op3 {
    type A0 = DI64
    type A1 = BufferInt
    type A2 = Int
    type T = DI32
    def op(a: DI64, b: BufferInt, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[DI32] = for {
      a <- bufferIfNeeded(a)
    } yield Left(a.hasMissingInGroup(b, c))
  }

}
