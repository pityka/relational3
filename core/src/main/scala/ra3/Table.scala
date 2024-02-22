package ra3

import tasks.TaskSystemComponents
import cats.effect.IO
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker

private[ra3] case class PartitionData(
    columns: Seq[Int],
    partitionBase: Int,
    partitionMapOverSegments: Vector[Int]
) {
  override def toString =
    s"Partitioned over $columns with base $partitionBase ($partitionMapOverSegments)"
  def numPartitions =
    (1 to columns.size).foldLeft(1)((acc, _) => acc * partitionBase)
  def partitionIdxOfPrefix(prefix: Seq[Int]): Vector[Int] = {
    assert(columns.startsWith(prefix))
    val div = (1 to (columns.size - prefix.size)).foldLeft(1)((acc, _) =>
      acc * partitionBase
    )
    partitionMapOverSegments.map { p1 =>
      p1 / div
    }
  }
  def isPrefixOf(other: PartitionData) =
    other.partitionBase == this.partitionBase && other.columns.startsWith(
      columns
    )
}

// Segments in the same table are aligned: each column holds the same number of segments of the same size
case class Table(
    private[ra3] columns: Vector[Column],
    colNames: Vector[String],
    private[ra3] uniqueId: String,
    private[ra3] partitions: Option[PartitionData]
) extends RelationalAlgebra {
  assert(columns.map(_.segments.map(_.numElems)).distinct.size == 1)

  def numCols = columns.size
  def numRows =
    columns.map(_.segments.map(_.numElems.toLong).sum).headOption.getOrElse(0L)
  def segmentation =
    columns.map(_.segments.map(_.numElems)).headOption.getOrElse(Nil)
  def mapColIndex(f: String => String) = copy(colNames = colNames.map(f))
  def apply(s: String) = columns(
    colNames.zipWithIndex
      .find(_._1 == s)
      .getOrElse(throw new NoSuchElementException(s"column $s not found"))
      ._2
  )
  def apply(i: Int) = columns(i)

  override def toString =
    f"Table($uniqueId: $numRows%,d x $numCols%,d . num segments: ${segmentation.size}%,d (segment sizes min/max:${segmentation.min}%,d/${segmentation.max}%,d)\nPartitioning: $partitions\n${colNames
        .zip(columns)
        .map { case (name, col) =>
          s"'$name'\t${col.tag}"
        }
        .mkString("\n")}\n)"

  def showSample(nrows: Int = 100, ncols: Int = 10)(implicit
      tsc: TaskSystemComponents
  ): IO[String] = {
    this.segmentation.zipWithIndex.find(_._1 > 0).map(_._2) match {
      case None => IO.pure("")
      case Some(segmentIdx) =>
        bufferSegment(segmentIdx).map(_.toStringFrame.stringify(nrows, ncols))
    }
  }
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

object Table {
  implicit val codec: JsonValueCodec[Table] = JsonCodecMaker.make

  def concatenate(
      others: Table*
  )(implicit tsc: TaskSystemComponents): IO[Table] = {
    val name = ts.MakeUniqueId.queue(
      others.head,
      s"concat",
      others.flatMap(_.columns)
    )
    name.map { name =>
      val all = others
      assert(all.map(_.colNames).distinct.size == 1)
      assert(all.map(_.columns.map(_.tag)).distinct.size == 1)
      val columns = all.head.columns.size
      val cols = 0 until columns map { cIdx =>
        all.map(_.columns(cIdx)).reduce(_ castAndConcatenate _)
      } toVector

      Table(cols, all.head.colNames, name, None)
    }
  }
}
