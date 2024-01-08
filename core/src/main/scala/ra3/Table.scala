package ra3

import tasks.TaskSystemComponents
import cats.effect.IO

// Segments in the same table are aligned: each column holds the same number of segments of the same size
case class Table(
    columns: Vector[Column],
    colNames: Vector[String],
    uniqueId: String
) extends RelationalAlgebra {
  assert(columns.map(_.segments.map(_.numElems)).distinct.size == 1)
  def numCols = columns.size
  def numRows =
    columns.map(_.segments.map(_.numElems.toLong).sum).headOption.getOrElse(0L)
  def segmentation =
    columns.map(_.segments.map(_.numElems)).headOption.getOrElse(Nil)
  def mapColIndex(f: String => String) = copy(colNames = colNames.map(f))

  override def toString =
    s"Table($uniqueId: $numRows x $numCols . segments: ${segmentation.size} (${segmentation.min}/${segmentation.max})\n${colNames
        .zip(columns)
        .map { case (name, col) =>
          s"'$name'\t${col.tag}"
        }
        .mkString("\n")}\n)"
  def bufferSegment(
      idx: Int
  )(implicit tsc: TaskSystemComponents): IO[BufferedTable] = {
    IO.parSequenceN(32)(columns.map(_.segments(idx).buffer: IO[Buffer]))
      .map { buffers =>
        BufferedTable(buffers, colNames)
      }
  }
  def bufferStream(implicit
      tsc: TaskSystemComponents
  ): fs2.Stream[IO, BufferedTable] = {
    if (columns.isEmpty) fs2.Stream.empty
    else
      fs2.Stream
        .apply(0 until columns.head.segments.size: _*)
        .evalMap(idx => bufferSegment(idx))
  }
  def stringify(segmentIdx: Int = 0, nrows: Int = 10, ncols: Int = 10)(implicit
      tsc: TaskSystemComponents
  ) = bufferSegment(segmentIdx).map(_.toFrame.stringify(nrows, ncols))

  def addColumnFromSeq(tag: ColumnTag, columnName: String)(
      elems: Seq[tag.Elem]
  )(implicit tsc: TaskSystemComponents): IO[Table] = {
    assert(numRows == elems.length.toLong)
    val idx = columns.size
    val segmented = segmentation
      .foldLeft((elems, Vector.empty[Seq[tag.Elem]])) {
        case ((rest, acc), size) => rest.drop(size) -> (acc :+ rest.take(size))
      }
      ._2
      .map(_.toSeq)
    val col = tag.makeColumnFromSeq(this.uniqueId, idx)(segmented)
    col.flatMap { col =>
      this.addColOfSameSegmentation(col, columnName)
    }
  }
}
