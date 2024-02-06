package ra3
import ra3.lang.Expr
private[ra3] class ColWithNameTyped[T <: ColumnTag](
    uniqueId: String,
    colIdx: Int,
    protected val tag: T
) {
  def apply[T1](body: ra3.lang.Expr {
    type T = Either[tag.BufferType,Seq[tag.SegmentType]]
  } => ra3.lang.Expr { type T = T1 }): Expr { type T = T1 } =
    ra3.lang.global(ra3.lang.ColumnKey(uniqueId, colIdx)).apply[T1](body)
}
private[ra3] class ColWithNameTyped2[T <: ColumnTag](
    protected val col: T,
    col1: (String, Int),
    col2: (String, Int)
) {
  type E = ra3.lang.Expr {
    type T = Either[col.BufferType,Seq[col.SegmentType]]
  }
  def apply[T1](
      body: (E, E) => ra3.lang.Expr { type T = T1 }
  ): Expr { type T = T1 } =
    ra3.lang
      .global(ra3.lang.ColumnKey(col1._1, col1._2))
      .apply[T1](e1 =>
        ra3.lang
          .global(ra3.lang.ColumnKey(col2._1, col2._2))
          .apply[T1](e2 => body(e1, e2))
      )
}
private[ra3] class ColWithNameTyped3[T <: ColumnTag](
    protected val col: T,
    col1: (String, Int),
    col2: (String, Int),
    col3: (String, Int)
) {
  type E = ra3.lang.Expr {
    type T = Either[col.BufferType,Seq[col.SegmentType]]
  }
  def apply[T1](
      body: (E, E, E) => ra3.lang.Expr { type T = T1 }
  ): Expr { type T = T1 } =
    ra3.lang
      .global(ra3.lang.ColumnKey(col1._1, col1._2))
      .apply[T1](e1 =>
        ra3.lang
          .global(ra3.lang.ColumnKey(col2._1, col2._2))
          .apply[T1](e2 =>
            ra3.lang
              .global(ra3.lang.ColumnKey(col3._1, col3._2))
              .apply[T1](e3 => body(e1, e2, e3))
          )
      )
}

case class TableReference(
    uniqueId: String,
    colTags: Seq[ColumnTag],
    colNames: Seq[String]
) {
  private def findIdx(n: String) = colNames.zipWithIndex
    .find(_._1 == n)
    .getOrElse(throw new NoSuchElementException(s"column $n not found"))
    ._2
  def use[T <: ColumnTag](n: String) = {
    val idx = findIdx(n)
    new ColWithNameTyped[T](
      uniqueId,
      idx,
      colTags(idx).as[T]
    )
  }
  def use[T <: ColumnTag](n1: String, n2: String) = {
    val idx1 = findIdx(n1)
    val idx2 = findIdx(n2)
    assert(colTags(idx1) == colTags(idx2))
    new ColWithNameTyped2[T](
      colTags(idx1).as[T],
      (uniqueId, idx1),
      (uniqueId, idx2)
    )
  }
  def use[T <: ColumnTag](n1: String, n2: String, n3: String) = {
    val idx1 = findIdx(n1)
    val idx2 = findIdx(n2)
    val idx3 = findIdx(n3)
    assert(colTags(idx1) == colTags(idx2))
    assert(colTags(idx1) == colTags(idx3))
    new ColWithNameTyped3[T](
      colTags(idx1).as[T],
      (uniqueId, idx1),
      (uniqueId, idx2),
      (uniqueId, idx3)
    )
  }
  def use[T <: ColumnTag](n: Int) =
    new ColWithNameTyped[T](
      uniqueId,
      n,
      colTags(n).as[T]
    )
  def use[T <: ColumnTag](n1: Int, n2: Int) = {
    assert(colTags(n1) == colTags(n2))
    new ColWithNameTyped2[T](
      colTags(n1).as[T],
      (uniqueId, n1),
      (uniqueId, n2)
    )
  }
  def use[T <: ColumnTag](n1: Int, n2: Int, n3: Int) = {
    assert(colTags(n1) == colTags(n2))
    assert(colTags(n1) == colTags(n3))
    new ColWithNameTyped3[T](
      colTags(n1).as[T],
      (uniqueId, n1),
      (uniqueId, n2),
      (uniqueId, n3),
    )
  }
}
