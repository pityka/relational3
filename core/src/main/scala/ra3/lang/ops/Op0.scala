package ra3.lang.ops
import cats.effect.IO
import tasks.TaskSystemComponents
import ra3.*
import ra3.lang.util.bufferIfNeededWithPrecondition
import ra3.Utils.*
import ra3.lang.ReturnValueTuple

sealed trait Op0 {
  type T

  def op(): T
}

object Op0 {
  // not with type parameter because we have to serialize these

  case class ConstantI64(s: Long) extends Op0 {
    type T = Long
    def op() =
      s
  }

  case class ConstantI32(s: Int) extends Op0 {
    type T = Int
    def op() =
      s
  }

  case class ConstantF64(s: Double) extends Op0 {
    type T = Double
    def op() =
      s
  }

  case class ConstantString(s: String) extends Op0 {
    type T = String
    def op() =
      s
  }

  case class ConstantInstant(s: java.time.Instant) extends Op0 {
    type T = java.time.Instant
    def op() =
      s
  }
  case class ConstantI64S(s: Set[Long]) extends Op0 {
    type T = Set[Long]
    def op() =
      s
  }

  case class ConstantI32S(s: Set[Int]) extends Op0 {
    type T = Set[Int]
    def op() =
      s
  }

  case class ConstantF64S(s: Set[Double]) extends Op0 {
    type T = Set[Double]
    def op() =
      s
  }

  case class ConstantStringS(s: Set[String]) extends Op0 {
    type T = Set[String]
    def op() =
      s
  }

  case class ConstantInstantS(s: Set[java.time.Instant]) extends Op0 {
    type T = Set[java.time.Instant]
    def op() =
      s
  }

  case object ConstantEmptyReturnValue extends Op0 {
    type T = ReturnValueTuple[EmptyTuple, EmptyTuple]
    def op() = ReturnValueTuple.empty
  }

}
