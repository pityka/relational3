package ra3

import tasks.TaskSystemComponents
import cats.effect.IO

// Segments in the same table are aligned: each column holds the same number of segments of the same size
case class Table(
    columns: Vector[Column],
    colNames: Vector[String],
    uniqueId: String
) extends RelationalAlgebra {
  override def toString = s"Table($uniqueId:\n${colNames
      .zip(columns)
      .map { case (name, col) =>
        s"'$name'\t$col"
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
  def stringify(segmentIdx: Int = 0, nrows: Int = 10, ncols: Int = 10)(implicit
      tsc: TaskSystemComponents
  ) = bufferSegment(segmentIdx).map(_.toFrame.stringify(nrows, ncols))
}
