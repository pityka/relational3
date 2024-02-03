package ra3.lang.ops
import ra3.BufferInt
private[lang] sealed trait Op1 {
  type A0
  type T
  def op(a: A0): T
}

private[lang] object Op1 {

  object List1 extends Op1 {
    type A0
    type T = List[A0]
    def op(a: A0) = List(a)
  }

  case object MkUnnamedColumnSpec extends Op1 {
    type A0
    type T = ra3.lang.UnnamedColumn[A0]
    def op(a: A0) = ra3.lang.UnnamedColumn(a)
  }

  sealed trait Op1II extends Op1 {
    type A0 = Int
    type T = Int
  }

  case object AbsOp extends Op1II {
    def op(a: Int): Int = math.abs(a)
  }
  case object ToString extends Op1 {
    type A0 = Int
    type T = String
    def op(a: Int): String = a.toString
  }

  sealed trait BufferOp1II extends Op1 {
    type A0 = ra3.BufferInt
    type T = ra3.BufferInt
  }

  case object BufferAbsOpI extends BufferOp1II {
    def op(a: BufferInt): BufferInt = a.elementwise_abs
  }

}
