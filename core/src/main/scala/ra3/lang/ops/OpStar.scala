package ra3.lang.ops

private[lang] sealed trait OpStar {
  type A
  type T
  def op(a: A*): T
}

object MkList extends OpStar {
    type A
    type T = List[A]
    def op(a: A*) = List(a:_*)
  }
object MkSelect extends OpStar {
    type A = ra3.lang.ColumnSpec
    type T = ra3.lang.ReturnValue
    def op(a: A*) = ra3.lang.ReturnValue(List(a:_*),None)
  }
