package ra3

case class BufferedTable(
    columns: Vector[Buffer],
    colNames: Vector[String]
) {
  def toFrame = toStringFrame
  def toHomogeneousFrameWithRowIndex(rowIndexCol: Int, rowIndexTag: ColumnTag)(
      tag: ColumnTag
  )(implicit
      st: org.saddle.ST[tag.Elem],
      st2: org.saddle.ST[rowIndexTag.Elem],
      ord: org.saddle.ORD[rowIndexTag.Elem]
  ) = {
    import org.saddle._
    val header = columns(rowIndexCol).as(rowIndexTag).toSeq.toVec

    Frame(
      columns.zipWithIndex
        .filterNot(v => v._2 != rowIndexCol)
        .map(_._1)
        .map { col =>
          if (col.tag == tag) Some(col.as(tag).toSeq.toVec)
          else None
        }
        .collect { case Some(x) => x }: _*
    ).setRowIndex(Index(header.toArray))

  }
  def toHomogeneousFrame(
      tag: ColumnTag
  )(implicit st: org.saddle.ST[tag.Elem]) = {
    import org.saddle._
    Frame(columns.map(_.as(tag).toSeq.toVec): _*)
      .setColIndex(colNames.toIndex)
  }
  def toStringFrame = {
    import org.saddle._
    Frame(columns.map { buffer =>
      buffer.tag match {
        case x if x == ra3.ColumnTag.Instant =>
          buffer
            .as(ra3.ColumnTag.Instant)
            .toSeq
            .map(v => java.time.Instant.ofEpochMilli(v).toString)
            .toVec
        case _ => {
          val ar = Array.ofDim[String](buffer.length)
          var i = 0
          val n = ar.length
          while (i < n) {
            if (!buffer.isMissing(i)) {
              ar(i) = buffer.elementAsCharSequence(i).toString
            }
            i += 1
          }
          ar.toVec
        }
      }
    }: _*)
      .setColIndex(colNames.toIndex)
  }

}
