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

  def lift = ra3.tablelang.TableExpr.Const(this)
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
  def segmentation =
    columns
      .map(c => c.tag.segments(c.column).map(_.numElems))
      .headOption
      .getOrElse(Nil)

  def mapColIndex(f: String => String) = copy(colNames = colNames.map(f))

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
        bufferSegment(segmentIdx).map(_.toStringFrame.stringify(nrows, ncols))
    }
  }

  /** Returns all columns of one segment number as a BufferedTable
    *
    * Reads the all columns of the corresponding segment number to memory.
    */
  def bufferSegment(
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

  /** Returns a string summary of the table without data itself
    *
    * Use the showSample to actually show some data
    */
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
    val col = tag
      .makeColumnFromSeq(this.uniqueId, idx)(segmented)
      .map(tag.makeTaggedColumn)
    col.flatMap { col =>
      this.addColOfSameSegmentation(col, columnName)
    }
  }

  import ra3.tablelang.TableExpr

  /** Variable assigning let expression where the assigned part is a single
    * Table
    */
  def schema[T1: NotNothing] =
    TableExpr.const[T1 *: EmptyTuple](this)

  /** Variable assigning let expression where the assigned part is a single
    * Table
    */
  def schema[T1: NotNothing, T2: NotNothing] =
    TableExpr.const[(T1, T2)](this)

  /** Variable assigning let expression where the assigned part is a single
    * Table
    */
  def schema[T1: NotNothing, T2: NotNothing, T3: NotNothing] =
    TableExpr.const[(T1, T2, T3)](this)

  def schema[T1: NotNothing, T2: NotNothing, T3: NotNothing, T4: NotNothing] =
    TableExpr.const[(T1, T2, T3, T4)](this)

  def schema[
      T1: NotNothing,
      T2: NotNothing,
      T3: NotNothing,
      T4: NotNothing,
      T5: NotNothing
  ] =
    TableExpr.const[(T1, T2, T3, T4, T5)](this)
  def schema[T0 <: Tuple] =
    TableExpr.const[T0](this)

  import ra3.lang.Expr

  // /** Variable assigning let expression with column decomposition
  //   *
  //   * The assigned expression is a single table. The receiver is a typed
  //   * variable referencing the first column of the table.
  //   *
  //   * Care must be taken annotate type of the column correctly, otherwise
  //   * runtime error will occur.
  //   */
  // def let[T0: NotNothing](
  //     body: [R] => (Expr.DelayedIdent { type T = T0 }) => TableExpr{type T = R}
  // ) = {
  //   schema[T0].in { t =>
  //     t.in[T0](body)

  //   }
  // }
  // /** Variable assigning let expression with column decomposition
  //   *
  //   * The assigned expression is a single table. The receiver is a typed
  //   * variable referencing the first column of the table.
  //   *
  //   * Care must be taken annotate type of the column correctly, otherwise
  //   * runtime error will occur.
  //   */
  // def let[
  //   T0: NotNothing,
  //   T1: NotNothing,
  //   ](
  //     body: [R] => (
  //       Expr.DelayedIdent { type T = T0 },
  //       Expr.DelayedIdent { type T = T1 }
  //       ) => TableExpr{type T = R}
  // ): TableExpr{type T = R} = {
  //   schema[T0,T1] { t =>
  //     t.in[T0,T1](body)

  //   }
  // }
  // /** Variable assigning let expression with column decomposition
  //   *
  //   * The assigned expression is a single table. The receiver is a typed
  //   * variable referencing the first column of the table.
  //   *
  //   * Care must be taken annotate type of the column correctly, otherwise
  //   * runtime error will occur.
  //   */
  // def let[
  //   T0: NotNothing,
  //   T1: NotNothing,
  //   T2: NotNothing,
  //   ](
  //     body: [R] =>  (
  //       Expr.DelayedIdent { type T = T0 },
  //       Expr.DelayedIdent { type T = T1 },
  //       Expr.DelayedIdent { type T = T2 }
  //       ) => TableExpr{type T = R}
  // ): TableExpr{type T = R} = {
  //   schema[T0,T1,T2] { t =>
  //     t.in[T0,T1,T2](body)

  //   }
  // }
  // /** Variable assigning let expression with column decomposition
  //   *
  //   * The assigned expression is a single table. The receiver is a typed
  //   * variable referencing the first column of the table.
  //   *
  //   * Care must be taken annotate type of the column correctly, otherwise
  //   * runtime error will occur.
  //   */
  // def let[
  //   T0: NotNothing,
  //   T1: NotNothing,
  //   T2: NotNothing,
  //   T3: NotNothing,
  //   ](
  //     body: [R] => (
  //       Expr.DelayedIdent { type T = T0 },
  //       Expr.DelayedIdent { type T = T1 },
  //       Expr.DelayedIdent { type T = T2 },
  //       Expr.DelayedIdent { type T = T3 }
  //       ) => TableExpr{type T = R}
  // ):TableExpr{type T = R}= {
  //   schema[T0,T1,T2,T3] { t =>
  //     t.in[T0,T1,T2,T3](body)

  //   }
  // }
}

object Table {
  implicit val codec: JsonValueCodec[Table] = JsonCodecMaker.make

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
