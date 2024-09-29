package ra3.lang.ops
import cats.effect.IO
import tasks.TaskSystemComponents
import ra3.*
import ra3.lang.bufferIfNeededWithPrecondition
import ra3.Utils.* 
import ra3.lang.ReturnValueTuple

private[ra3] sealed trait Op0 {
  type T

  
  def op(): T
}

private[ra3] object Op0 {

  class Constant[T0](s:T0) extends Op0 {
    type T = T0
    def op() =
      s
  }
}