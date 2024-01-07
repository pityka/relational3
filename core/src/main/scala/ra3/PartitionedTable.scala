package ra3

import cats.effect.IO
import tasks.TaskSystemComponents

case class PartitionedTable(columns: Vector[Column]) {
  assert(columns.map(_.segments.map(_.numElems)).distinct.size == 1)

  def numCols = columns.size
  def numRows =
    columns.map(_.segments.map(_.numElems.toLong).sum).headOption.getOrElse(0L)
  def segmentation =
    columns.map(_.segments.map(_.numElems)).headOption.getOrElse(Nil)
  override def toString =
    s"PartitionedTable($numRows x $numCols . segments: ${segmentation.size} (${segmentation.min}/${segmentation.max})\n${columns.zipWithIndex
        .map { case (col, idx) =>
          s"$idx.\t${col.tag}"
        }
        .mkString("\n")}\n)"
  def concatenate(other: PartitionedTable) = {
    assert(columns.size == other.columns.size)
    assert(columns.map(_.tag) == other.columns.map(_.tag))
    PartitionedTable(
      columns.zip(other.columns).map { case (a, b) => a castAndConcatenate b }
    )
  }

  def bufferSegment(
      idx: Int
  )(implicit tsc: TaskSystemComponents): IO[BufferedTable] = {
    IO.parSequenceN(32)(columns.map(_.segments(idx).buffer: IO[Buffer]))
      .map { buffers =>
        BufferedTable(buffers, columns.zipWithIndex.map(v => s"V${v._2}"))
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
}
