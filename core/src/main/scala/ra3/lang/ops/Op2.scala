package ra3.lang.ops
import ra3.BufferInt
import ra3.BufferIntConstant
import ra3.lang.ColumnName
import ra3.lang.ReturnValue
private[lang] sealed trait Op2 {
  type A0
  type A1
  type T
  def op(a: A0, b: A1): T
}

private[lang] object Op2 {

  object Tap extends Op2 {
    type A0  
    type A1 = String  
    type T = A0
    def op(a: A0, b: A1) = {
      
      scribe.info(s"$b : ${a.toString}")
      a
    }
  }

   case object Cons extends Op2 {
    type A 
    type A0 = A 
    type A1 = List[A]
    type T = List[A]
    def op(a: A,b: List[A]) = a::b
  }
   case object MkReturnWhere extends Op2 {
    type A0 = ra3.lang.ReturnValue
    type A1 = BufferInt
    type T = ReturnValue
    def op(a: A0,b: A1) = ReturnValue(a.projections,a.filter match {
      case Some(f) => Some(b.elementwise_&&(f))
      case None => Some(b)
    })
  }
   case object WithColumnName extends Op2 {
    type A0
    type A1 = String
    type T = ColumnName[A0]
    def op(a: A0,b: String) = ColumnName(a,b)
  }
   sealed trait BufferOp2III extends Op2 {
    type A0 = ra3.BufferInt 
    type A1 = ra3.BufferInt 
    type T = ra3.BufferInt
  }
   sealed trait BufferOp2ICII extends Op2 {
    type A0 = ra3.BufferInt 
    type A1 = Int
    type T = ra3.BufferInt
  }

  
  case object BufferLtEqOpII extends BufferOp2III {
    def op(a:BufferInt,b:BufferInt) : BufferInt = a.elementwise_lteq(b)
  }
  case object BufferLtEqOpIcI extends BufferOp2ICII {
    def op(a:BufferInt,b:Int) : BufferInt = a.elementwise_lteq(BufferIntConstant(b,a.length))
  }

  sealed trait Op2III extends Op2 {
    type A0 = Int
    type A1 = Int
    type T = Int
  }

  case object AddOp extends Op2III {
    def op(a: Int, b: Int): Int = a + b
  }
  case object MinusOp extends Op2III {
    def op(a: Int, b: Int): Int = a - b
  }

}
