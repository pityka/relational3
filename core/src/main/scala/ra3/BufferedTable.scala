package ra3

case class BufferedTable(
    columns: Vector[Buffer],
    colNames: Vector[String]
) {
  def toFrame = {
    import org.saddle._
    Frame(columns.map(_.toSeq.map(_.toString).toVec): _*)
      .setColIndex(colNames.toIndex)
  }
  def toHomogeneousFrame(tag: ColumnTag)(implicit st: org.saddle.ST[tag.Elem]) = {
    import org.saddle._
    Frame(columns.map(_.as(tag).toSeq.toVec): _*)
      .setColIndex(colNames.toIndex)
  }
}