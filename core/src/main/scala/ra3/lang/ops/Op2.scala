package ra3.lang.ops
import ra3.BufferIntConstant
import ra3._
import ra3.lang._
import ra3.lang.DI32
import ra3.lang.ReturnValue
import ra3.lang.bufferIfNeeded
import tasks.TaskSystemComponents
import cats.effect.IO
private[lang] sealed trait Op2 {
  type A0
  type A1
  type T
  def op(a: A0, b: A1)(implicit tsc: TaskSystemComponents): IO[T]
}

private[lang] object Op2 {

  object Tap extends Op2 {
    type A0
    type A1 = String
    type T = A0
    def op(a: A0, b: A1)(implicit tsc: TaskSystemComponents) = IO {

      scribe.info(s"$b : ${a.toString}")
      a
    }
  }

  case object Cons extends Op2 {
    type A
    type A0 = A
    type A1 = List[A]
    type T = List[A]
    def op(a: A, b: List[A])(implicit tsc: TaskSystemComponents) =
      IO.pure(a :: b)
  }
  case object MkReturnWhere extends Op2 {
    type A0 = ra3.lang.ReturnValue
    type A1 = DI32
    type T = ReturnValue
    def op(a: A0, b: A1)(implicit tsc: TaskSystemComponents) = 
      (a.filter match {
        case None => IO.pure(Some(b)) 
        case Some(f) => for {
          f <- bufferIfNeeded(f)
          b <- bufferIfNeeded(b)
        } yield Some(Left(b.elementwise_&&(f)))
      }).map(f => ReturnValue(a.projections,f))
      
  }
  case object MkNamedColumnSpecChunk extends Op2 {
    type A0 = Either[Buffer,Seq[Segment]]
    type A1 = String
    type T = NamedColumnChunk
    def op(a: A0, b: String)(implicit tsc: TaskSystemComponents) =
      IO.pure(NamedColumnChunk(a, b))
  }
  case object MkNamedConstantI32 extends Op2 {
    type A0 = Int
    type A1 = String
    type T = NamedConstantI32
    def op(a: A0, b: String)(implicit tsc: TaskSystemComponents) =
      IO.pure(NamedConstantI32(a, b))
  }
  sealed trait ColumnOp2III extends Op2 {
    type A0 = ra3.lang.DI32
    type A1 = ra3.lang.DI32
    type T = ra3.lang.DI32
  }
  sealed trait ColumnOp2ICII extends Op2 {
    type A0 = ra3.lang.DI32
    type A1 = Int
    type T = ra3.lang.DI32
  }

  case object ColumnLtEqOpII extends ColumnOp2III {
    def op(a: DI32, b: DI32)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeeded(a)
        b <- bufferIfNeeded(b)
      } yield Left(a.elementwise_lteq(b))

    }
  }
  case object ColumnLtEqOpIcI extends ColumnOp2ICII {
    def op(a: DI32, b: Int)(implicit tsc: TaskSystemComponents): IO[DI32] = {
      for {
        a <- bufferIfNeeded(a)
      } yield Left(a.elementwise_lteq(BufferIntConstant(b, a.length)))
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

}
