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

  case object IfElse extends Op3III {
    def op(a: Int, b: Int, c: Int)(implicit
        tsc: TaskSystemComponents
    ): IO[Int] = IO.pure(if (a > 0) b else c)
  }

}
