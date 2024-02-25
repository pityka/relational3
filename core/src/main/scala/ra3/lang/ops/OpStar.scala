package ra3.lang.ops

private[ra3] sealed trait OpStar {
  type A
  type T
  def op(a: A*): T
}

private[ra3] object OpStar {

  object MkSelect extends OpStar {
    type A = ra3.lang.ColumnSpec
    type T = ra3.lang.ReturnValue
    def op(a: A*) = ra3.lang.ReturnValue(List(a: _*), None)
  }
}
