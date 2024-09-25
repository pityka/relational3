package ra3

case class BufferedTable(
    columns: Vector[Buffer],
    colNames: Vector[String]
) {
  def toFrame = toStringFrame
  def toHomogeneousFrameWithRowIndex[Rx,V](rowIndexCol: Int)(implicit
      st: org.saddle.ST[V],
      st2: org.saddle.ST[Rx],
      ord: org.saddle.ORD[Rx]
  ) = {
    import org.saddle.*
    val header = columns(rowIndexCol).toSeq.asInstanceOf[Seq[Rx]].toVec

    Frame(
      columns.zipWithIndex
        .filterNot(v => v._2 != rowIndexCol)
        .map(_._1)
        .map { col =>
          col.toSeq.asInstanceOf[Seq[V]].toVec
        }*
    ).setRowIndex(Index(header.toArray))

  }
 
  def toHomogeneousFrame( tag: ColumnTag)(implicit st: org.saddle.ST[tag.Elem]) = {
    import org.saddle.*
    Frame(columns.map(_.toSeq.asInstanceOf[Seq[tag.Elem]].toVec)*)
      .setColIndex(colNames.toIndex)
  }
  def toStringFrame = {
    import org.saddle.*
    Frame(columns.map { buffer =>
      buffer match {
        case buffer: BufferInstant =>
          buffer
            // .as(ra3.ColumnTag.Instant)
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
    }*)
      .setColIndex(colNames.toIndex)
  }

}
