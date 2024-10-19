package ra3

import tasks.TaskSystemComponents
import cats.effect.IO
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import scala.reflect.ClassTag

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

/** Reference to a set of aligned columns (i.e. a table) persisted onto
  * secondary storage.
  *
  * Each table must have a unique identifier, initially given by the importCsv
  * method.
  *
  * Tables have String column names.
  *
  * Tables consists of columns. Columns are stored as segments. Segments are the
  * unit of IO operations, i.e. ra3 never reads less then a segment into memory.
  * The in memory (buffered) counterpart of a segment is a Buffer. The maximum
  * number of elements in a segment is thus what is readable into a single java
  * array, that is shortly below 2^31.
  *
  * Each column of the same table has the same segmentation, i.e. they have the
  * same number of segments and their segments have the same size and those
  * segments are aligned.
  *
  * Segments store segment level statistics and some operations complete withour
  * buffering the segment.
  */
case class Table(
    private[ra3] columns: Vector[TaggedColumn],
    colNames: Vector[String],
    private[ra3] uniqueId: String,
    private[ra3] partitions: Option[PartitionData]
) extends RelationalAlgebra {

  assert(
    columns
      .map(c => c.tag.segments(c.column).map(_.numElems))
      .distinct
      .size == 1
  )

  def numCols = columns.size
  def numRows =
    columns
      .map(c => c.tag.segments(c.column).map(_.numElems.toLong).sum)
      .headOption
      .getOrElse(0L)

  /** Returns one integer list, of the same size as the number of segments,
    * items being the size of segments
    */
  private[ra3] def segmentation =
    columns
      .map(c => c.tag.segments(c.column).map(_.numElems))
      .headOption
      .getOrElse(Nil)

  private[ra3] def mapColIndex(f: String => String) =
    copy(colNames = colNames.map(f))

  /** Selects a column by name, or throws if not exists. */
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

  /** Renders a sample of the table's data content
    */
  def showSample(nrows: Int = 100, ncols: Int = 10)(implicit
      tsc: TaskSystemComponents
  ): IO[String] = {
    this.segmentation.zipWithIndex.find(_._1 > 0).map(_._2) match {
      case None => IO.pure("")
      case Some(segmentIdx) =>
        bufferSegment(segmentIdx).map(
          _.toStringFrame(nrows).stringify(nrows, ncols)
        )
    }
  }

  /** Returns all columns of one segment number as a BufferedTable
    *
    * Reads the all columns of the corresponding segment number to memory.
    */
  private[ra3] def bufferSegment(
      idx: Int
  )(implicit tsc: TaskSystemComponents): IO[BufferedTable] = {
    IO.parSequenceN(32)(columns.map { v =>
      val segment = v.tag.segments(v.column)(idx)
      v.tag.buffer(segment): IO[Buffer]
    }).map { buffers =>
      BufferedTable(buffers, colNames)
    }
  }

  /** Returns an fs2 Stream with a BufferedTable for each segment number */
  def bufferStream(implicit
      tsc: TaskSystemComponents
  ): fs2.Stream[IO, BufferedTable] = {
    if (columns.isEmpty) fs2.Stream.empty
    else {
      val c = columns.head
      fs2.Stream
        .apply(0 until c.tag.segments(c.column).size*)
        .evalMap(idx => bufferSegment(idx))
    }
  }

  /** Returns an fs2 Stream with a tuple for each element */
  transparent inline def stream[T <: Tuple](implicit
      tsc: TaskSystemComponents
  ): fs2.Stream[IO, T] = {
    bufferStream.flatMap(_.toTuples[T])
  }
  private[ra3] transparent inline def streamOfTuplesFromColumnChunks[
      T <: Tuple
  ](implicit
      tsc: TaskSystemComponents
  ) = {
    bufferStream.flatMap(_.toTuplesFromColumnChunks[T])
  }
  private[ra3] transparent inline def streamOfSingleColumnChunk[
      T
  ](implicit
      tsc: TaskSystemComponents
  ) = {
    bufferStream.map(_.toChunkedStream[T])
  }

  import ra3.tablelang.TableExpr

  /** Schema definition
    *
    * Entry point of TableExpr DSL
    */
  def as[T1 <: Tuple] =
    TableExpr.const[T1](this)

}

object Table {
  // $COVERAGE-OFF$
  implicit val codec: JsonValueCodec[Table] = JsonCodecMaker.make
  // $COVERAGE-ON$

  /** Concatenate the list of rows of multiple tables ('grows downwards')
    *
    * Same as ra3.concatenate
    */
  def concatenate(
      others: Table*
  )(implicit tsc: TaskSystemComponents): IO[Table] = {
    ra3.concatenate(others*)
  }
}
