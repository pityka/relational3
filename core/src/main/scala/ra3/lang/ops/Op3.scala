package ra3.lang.ops

import ra3.BufferInt

private[lang] sealed trait Op3 {
  type A0
  type A1
  type A2
  type T
  def op(a: A0, b: A1, c: A2): T
}

private[lang] object Op3 {

sealed trait Op3III extends Op3 {
  type A0 = Int
  type A1 = Int
  type A2 = Int
  type T = Int
}

case object BufferSumGroupsOpII extends Op3 {
  type A0 = BufferInt
  type A1 = BufferInt 
  type A2 = Int
  type T = BufferInt
    def op(a:BufferInt,b:BufferInt,c: Int) : BufferInt = a.sumGroups(b,c)
  }
case object BufferFirstGroupsOpIIi extends Op3 {
  type A0 = BufferInt
  type A1 = BufferInt 
  type A2 = Int
  type T = BufferInt
    def op(a:BufferInt,b:BufferInt,c: Int) : BufferInt = a.firstInGroup(b,c)
  }

case object IfElse extends Op3III {
  def op(a: Int, b: Int,c : Int): Int = if (a > 0) b else c 
}


}