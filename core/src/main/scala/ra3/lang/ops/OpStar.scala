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

  // object OpStar {
  //   object ColumnCatOpStr extends OpStar {
  //   type A = ra3.lang.DStr
  //   type T = ra3.lang.DStr
  //   def op(a: A*) : IO[ra3.lang.DStr] = for {
  //     a <- IO.parSequenceN(4)(a.map(ra3.lang.bufferIfNeeded))
  //   } yield {
  //     if (a.length == 0) ???
  //     else {
  //     val r = Array.ofDim[CharSequence](a.head.length)
    
  //     Left(BufferString(r))
  //     }
  //   }
  // }
  // }