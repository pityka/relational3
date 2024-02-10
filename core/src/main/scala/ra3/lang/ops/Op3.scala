package ra3.lang.ops

import ra3.BufferInt
import ra3.lang._
import tasks.TaskSystemComponents
import cats.effect._
private[lang] sealed trait Op3 {
  type A0
  type A1
  type A2
  type T
  def op(a: A0, b: A1, c: A2)(implicit tsc: TaskSystemComponents): IO[T]
}

private[lang] object Op3 {

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

  case object IfElse extends Op3III {
    def op(a: Int, b: Int, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[Int] = IO.pure(if (a > 0) b else c)
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

}
