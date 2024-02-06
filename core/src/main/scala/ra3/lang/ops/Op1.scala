package ra3.lang.ops
import cats.effect.IO
import tasks.TaskSystemComponents
import ra3._
private[lang] sealed trait Op1 {
  type A0
  type T

  def bufferBefore[
      B <: Buffer { type BufferType = B },
      S <: Segment { type SegmentType = S; type BufferType = B },
      C
  ](
      arg: Either[B, Seq[S]]
  )(fun: B => B)(implicit tsc: TaskSystemComponents): IO[Either[B, Seq[S]]] =
    arg match {
      case Left(b) => IO.pure(Left(fun(b)).withRight[Seq[S]])
      case Right(s) =>
        ra3.Utils.bufferMultiple(s).map(b => Left(fun(b)).withRight[Seq[S]])
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
    type A0 = Either[Buffer,Seq[Segment]]
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

  // sealed trait BufferOp1II extends Op1 {
  //   type A0 = ra3.BufferInt
  //   type T = ra3.BufferInt
  // }
  sealed trait ColumnOp1II extends Op1 {
    type A0 = ra3.lang.DI32
    type T = ra3.lang.DI32
  }

  // case object BufferAbsOpI extends BufferOp1II {
  //   def op(a: BufferInt)(implicit tsc: TaskSystemComponents): IO[BufferInt] =
  //     IO.pure(a.elementwise_abs)
  // }
  case object ColumnAbsOpI extends ColumnOp1II {
    def op(
        a: ra3.lang.DI32
    )(implicit tsc: TaskSystemComponents) = bufferBefore(a)(_.elementwise_abs)
  }

}
