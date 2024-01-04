package ra3

import cats.effect.IO
import fs2.Stream
import java.nio.ByteBuffer
import java.nio.channels.WritableByteChannel
import java.nio.ByteOrder
import java.nio.channels.ReadableByteChannel
import tasks.{TaskSystemComponents, SharedFile}
import cats.effect.kernel.Par
import scala.collection.immutable
import ra3.ts.EstimateCDF
import ra3.ts.MakeUniqueId
import ra3.ts.MergeCDFs

case class PartitionPath(
    partitionedOnColumns: Seq[Int],
    numPartitions: Int,
    partitionId: Int
)

case class LogicalPath(
    table: String,
    partition: Option[PartitionPath],
    segment: Int,
    column: Int
) {
  def appendToTable(suffix: String) = copy(table = table + suffix)
  override def toString = {
    val part = partition.map {
      case PartitionPath(by, pnum, pidx) if by.nonEmpty =>
        s"/partitions/${by.mkString("-")}/$pnum/$pidx"
      case PartitionPath(_, pnum, pidx) => s"/partitions//$pnum/$pidx"
    }
    s"$table$part/segments/$segment/columns/$column"
  }
}

sealed trait Statistic[T]

sealed trait Segment[T] {
  def buffer(implicit tsc: TaskSystemComponents): IO[Buffer[T]]
  def statistics: IO[Option[Statistic[T]]]
  def numElems: Int
}

object Segment {

  case class GroupMap(
      map: Segment[Int],
      numGroups: Int,
      groupSizes: Segment[Int]
  )

  import com.github.plokhotnyuk.jsoniter_scala.macros._
  import com.github.plokhotnyuk.jsoniter_scala.core._
  implicit val codec: JsonValueCodec[Segment[_]] = JsonCodecMaker.make
}

final case class SegmentDouble(sf: SharedFile, numElems: Int)
    extends Segment[Double] {

  override def buffer(implicit tsc: TaskSystemComponents): IO[Buffer[Double]] =
    ???

  override def statistics: IO[Option[Statistic[Double]]] = ???

}
final case class SegmentInt(sf: SharedFile, numElems: Int)
    extends Segment[Int] {

  override def buffer(implicit tsc: TaskSystemComponents): IO[BufferInt] = {
    // import fs2.interop.scodec.StreamDecoder
    // import scodec.codecs._
    // sf.stream.through(StreamDecoder.many(scodec.codecs.int32L).toPipeByte).chunkAll.compile.last.map{_.get.toArray}

    sf.bytes.map { byteVector =>
      val bb =
        byteVector.toByteBuffer.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer()
      val ar = Array.ofDim[Int](bb.remaining / 4)
      bb.get(ar)
      BufferInt(ar)
    }

  }

  def statistics: IO[Option[Statistic[Int]]] = IO.pure(None)

}

sealed trait ColumnDataType
case object IntDataType extends ColumnDataType

/** This should be short */
case class CDF(locations: Segment[_], values: SegmentInt) {
  def topK(k: Int, ascending: Boolean)(implicit tsc: TaskSystemComponents) = {
    val locs: IO[Buffer[_]] = locations.buffer
    val vals: IO[BufferInt] = values.buffer
    IO.both(locs, vals).map { case (locs, vals) =>
      val indexInLocs =
        if (ascending) vals.toSeq.zipWithIndex.find(_._1 >= k).map(_._2)
        else {

          val s = vals.toSeq
          val max = s.max
          s.zipWithIndex.reverse.find(_._1 <= (max - k)).map(_._2)
        }
      indexInLocs.map(idx => locs.take(BufferInt(Array(idx))))
    }

  }
}

case class Column(
    segments: Vector[Segment[_]],
    tpe: ColumnDataType
) extends ColumnOps {
  def ++(other: Column) = {
    assert(tpe == other.tpe)
    Column(segments ++ other.segments, tpe)
  }

  def estimateCDF(coverage: Double, numPointsPerSegment: Int)(implicit
      tsc: TaskSystemComponents
  ) = {
    assert(coverage > 0d)
    assert(coverage <= 1d)
    val total = segments.map(_.numElems.toLong).sum
    val numPick = math.min(1, (coverage * total).toLong)
    val shuffleSegments = scala.util.Random.shuffle(segments.zipWithIndex)
    val cumulative = shuffleSegments
      .map(v => (v, v._1.numElems))
      .scanLeft(0)((a, b) => (a + b._2))
    val pickSegments =
      shuffleSegments.zip(cumulative).takeWhile(_._2 < numPick).map(_._1)
    assert(pickSegments.size > 0)
    assert(pickSegments.size <= segments.size)
    MakeUniqueId.queue0("estimatecdf", List(this)).flatMap { uniqueId =>
      IO.parSequenceN(32)(pickSegments.map { case (segment, segmentIdx) =>
        EstimateCDF.queue(
          segment,
          numPointsPerSegment,
          LogicalPath(
            table = uniqueId,
            partition = None,
            segment = segmentIdx,
            column = 0
          )
        )

      }).flatMap { cdfs =>
        MergeCDFs.queue(
          cdfs,
          LogicalPath(
            table = uniqueId + ".merged",
            partition = None,
            segment = 0,
            column = 0
          )
        )
      }
    }
  }

}

// Segments in the same table are aligned: each column holds the same number of segments of the same size
case class Table(
    columns: Vector[Column],
    colNames: Vector[String],
    uniqueId: String
) extends RelationalAlgebra {
  def bufferSegment(
      idx: Int
  )(implicit tsc: TaskSystemComponents): IO[BufferedTable] = {
    IO.parSequenceN(32)(columns.map(_.segments(idx).buffer)).map { buffers =>
      BufferedTable(buffers, colNames)
    }
  }
  def stringify(nrows: Int,ncols:Int,segment: Int)(implicit tsc: TaskSystemComponents) = bufferSegment(segment).map(_.toFrame.stringify(nrows,ncols))
}

case class PartitionedTable(columns: Vector[Column]) {
  def concatenate(other: PartitionedTable) = {
    assert(columns.size == other.columns.size)
    assert(columns.map(_.tpe) == other.columns.map(_.tpe))
    PartitionedTable(columns.zip(other.columns).map { case (a, b) => a ++ b })
  }
}

case class GroupedTable(
    table: Table,
    groups: Seq[Segment.GroupMap]
) {

  def extractGroups(implicit tsc: TaskSystemComponents): IO[Seq[Table]] = {

    val name = ts.MakeUniqueId.queue(
      this.table,
      s"extractgroups",
      List(Column(this.groups.map(_.map).toVector, IntDataType))
    )
    name.flatMap { name =>
      IO.parSequenceN(math.min(32, this.table.columns.size))(
        this.table.columns.zipWithIndex.map { case (column, columnIdx) =>
          assert(column.segments.size == this.groups.size)
          IO.parSequenceN(math.min(32, column.segments.size))(
            column.segments.zip(this.groups).zipWithIndex.map {
              case ((inputSegment, groupMap), segmentIdx) =>
                assert(inputSegment.numElems == groupMap.map.numElems)
                ts.ExtractGroups.queue(
                  inputSegment,
                  groupMap.map.asInstanceOf[SegmentInt],
                  groupMap.numGroups,
                  LogicalPath(
                    table = name,
                    partition = None,
                    segment = segmentIdx,
                    column = columnIdx
                  )
                )

            }
          ).map {
            // segment x group
            segments =>
              // groups x segment
              val t = segments.transpose
              t.map { segments =>
                Column(segments, column.tpe)
              }
          }
        }
      ).map {
        // columns x groups
        columns =>
          // groups x columns
          val t = columns.transpose
          t.zipWithIndex.map { case (columns, gIdx) =>
            Table(columns, this.table.colNames, name + "-g" + gIdx)
          }
      }
    }

  }

  def reduceGroups(reductions: Seq[ReductionOp])(implicit
      tsc: TaskSystemComponents
  ): IO[Table] = {
    assert(this.table.columns.size == reductions.size)
    val name = ts.MakeUniqueId.queue(
      this.table,
      s"reduce-${reductions.map(_.id).mkString("-")}",
      List(Column(this.groups.map(_.map).toVector, IntDataType))
    )
    name.flatMap { name =>
      IO.parSequenceN(math.min(32, this.table.columns.size))(
        this.table.columns.zip(reductions).map { case (column, reduction) =>
          assert(column.segments.size == this.groups.size)
          IO.parSequenceN(math.min(32, column.segments.size))(
            column.segments.zip(this.groups).map {
              case (inputSegment, groupMap) =>
                assert(inputSegment.numElems == groupMap.map.numElems)
                reduction.reduce(inputSegment, groupMap)

            }
          ).map(segments => Column(segments, column.tpe))
        }
      ).map { columns =>
        Table(columns, this.table.colNames, name)
      }
    }
  }
}

case class BufferedTable(
    columns: Vector[Buffer[_]],
    colNames: Vector[String]
) {
  def toFrame = {
    import org.saddle._ 
    Frame(columns.map(_.toSeq.map(_.toString).toVec):_*).setColIndex(colNames.toIndex)
  }
}

trait RelationalAlgebra { self: Table =>

  /**   - For each aligned index segment, buffer it
    *   - For each column
    *   - For each segment in the column
    *   - Buffer column segment
    *   - Apply buffered predicate segment to buffered column segment
    *   - Write applied buffer to segment and upload
    *
    * @param indexes
    *   for each segment
    * @return
    */
  def take(indexes: Column)(implicit tsc: TaskSystemComponents): IO[Table] = {
    assert(self.columns.head.segments.size == indexes.segments.size)
    ts.MakeUniqueId.queue(self, "take", List(indexes)).flatMap { name =>
      IO.parTraverseN(math.min(32, indexes.segments.size))(
        indexes.segments.map(_.asInstanceOf[SegmentInt]).zipWithIndex
      ) { case (segment, segmentIdx) =>
        IO.parTraverseN(math.min(32, self.columns.size))(
          self.columns.zipWithIndex
        ) { case (column, columnIdx) =>
          ts.TakeIndex.queue(
            input = column.segments(segmentIdx),
            idx = segment,
            outputPath = LogicalPath(name, None, segmentIdx, columnIdx)
          )
        }

      }.map(vs =>
        Table(
          vs.transpose.zip(self.columns.map(_.tpe)).map {
            case (segments, tpe) =>
              Column(segments, tpe)
          },
          self.colNames,
          name
        )
      )
    }
  }

  /**   - Align predicate segment with table segmentation
    *   - For each aligned predicate segment, buffer it
    *   - For each column
    *   - For each segment in the column
    *   - Buffer column segment
    *   - Apply buffered predicate segment to buffered column segment
    *   - Write applied buffer to local segment
    *   - Resegment
    *
    * Variant which takes BufferedTable => BufferInt
    *
    * @param predicate
    * @return
    */
  def rfilter(
      predicate: Column
  )(implicit tsc: TaskSystemComponents): IO[Table] = {
    assert(self.columns.head.segments.size == predicate.segments.size)
    ts.MakeUniqueId.queue(self, "rfilter", List(predicate)).flatMap { name =>
      IO.parTraverseN(math.min(32, predicate.segments.size))(
        predicate.segments.map(_.asInstanceOf[SegmentInt]).zipWithIndex
      ) { case (segment, segmentIdx) =>
        IO.parTraverseN(math.min(32, self.columns.size))(
          self.columns.zipWithIndex
        ) { case (column, columnIdx) =>
          ts.Filter.queue(
            input = column.segments(segmentIdx),
            predicate = segment,
            outputPath = LogicalPath(name, None, segmentIdx, columnIdx)
          )
        }

      }.map(vs =>
        Table(
          vs.transpose.zip(self.columns.map(_.tpe)).map {
            case (segments, tpe) =>
              Column(segments, tpe)
          },
          self.colNames,
          name
        )
      )
    }
  }
  def rfilterInEquality(
      columnIdx: Int,
      cutoff: Segment[_],
      lessThan: Boolean
  )(implicit tsc: TaskSystemComponents): IO[Table] = {
    val comparisonColumn = self.columns(columnIdx)

    ts.MakeUniqueId
      .queue(
        self,
        "rfilterinequality",
        List(Column(Vector(cutoff), comparisonColumn.tpe))
      )
      .flatMap { name =>
        IO.parTraverseN(math.min(32, comparisonColumn.segments.size))(
          comparisonColumn.segments.zipWithIndex
        ) { case (comparisonSegment, segmentIdx) =>
          IO.parTraverseN(math.min(32, self.columns.size))(
            self.columns.zipWithIndex
          ) { case (column, columnIdx) =>
            ts.FilterInequality.queue(
              comparisonSegment = comparisonSegment,
              input = column.segments(segmentIdx),
              cutoff = cutoff,
              outputPath = LogicalPath(name, None, segmentIdx, columnIdx),
              lessThan = lessThan
            )
          }

        }.map(vs =>
          Table(
            vs.transpose.zip(self.columns.map(_.tpe)).map {
              case (segments, tpe) =>
                Column(segments, tpe)
            },
            self.colNames,
            name
          )
        )
      }
  }

  def partition(
      columnIdx: Seq[Int],
      numPartitions: Int
  )(implicit tsc: TaskSystemComponents): IO[Vector[PartitionedTable]] = {
    assert(columnIdx.nonEmpty)
    val partitionColumns = columnIdx.map(columns.apply)
    val numSegments = partitionColumns.head.segments.size
    val segmentIdxs = (0 until numSegments).toVector
    IO.parTraverseN(math.min(32, numSegments))(segmentIdxs) { segmentIdx =>
      val segmentsOfP = partitionColumns.map(_.segments(segmentIdx))
      val partitionMap = ts.MakePartitionMap.queue(
        segmentsOfP,
        numPartitions,
        LogicalPath(
          table = self.uniqueId + "-partitionmap",
          partition = Some(PartitionPath(columnIdx, numPartitions, 0)),
          segment = segmentIdx,
          column = 0
        )
      )
      partitionMap.flatMap { partitionMap =>
        IO.parTraverseN(math.min(32, columns.size))(columns.zipWithIndex) {
          case (column, currentColumnIdx) =>
            IO.parSequenceN(numPartitions)((0 until numPartitions).toList map {
              pIdx =>
                ts.TakePartition.queue(
                  input = column.segments(segmentIdx),
                  partitionMap = partitionMap,
                  pIdx = pIdx,
                  outputPath = LogicalPath(
                    table = self.uniqueId,
                    partition =
                      Some(PartitionPath(columnIdx, numPartitions, pIdx)),
                    segment = segmentIdx,
                    column = currentColumnIdx
                  )
                )
            })

        }
      }
    }.map {
      // segment x column x partition
      segments =>
        // partition x column x segment
        val transposed = segments.transpose.map(_.transpose).transpose

        transposed.map { columns =>
          PartitionedTable(
            columns.zipWithIndex.map { case (segments, columnIdx) =>
              Column(segments, self.columns(columnIdx).tpe)
            }
          )
        }

    }
  }

  /**   - Partition both tables by join column
    *   - For each partition of both input tables
    *   - Buffer the partition completely (all segments, all columns)
    *   - Join buffered tables in memory, use saddle's Index?
    *   - concat joined partitions
    * @param other
    * @param how
    * @return
    */
  def equijoin(
      other: Table,
      joinColumnSelf: Int,
      joinColumnOther: Int,
      how: String,
      numPartitions: Int
  )(implicit tsc: TaskSystemComponents) = {
    assert(
      self.columns(joinColumnSelf).tpe == other.columns(joinColumnOther).tpe
    )
    val name = ts.MakeUniqueId.queue2(
      self,
      other,
      s"join-$how-$numPartitions-$joinColumnSelf-$joinColumnOther",
      Nil
    )
    val pSelf = self.partition(List(joinColumnSelf), numPartitions)
    val pOther = other.partition(List(joinColumnOther), numPartitions)
    IO.both(name, IO.both(pSelf, pOther))
      .flatMap { case (name, (pSelf, pOther)) =>
        assert(pSelf.size == pOther.size)
        val joinedColumnsAndPartitions =
          IO.parSequenceN(32)((pSelf zip pOther).zipWithIndex.map {
            case ((pSelf, pOther), pIdx) =>
              assert(pSelf.columns.size == pOther.columns.size)
              val pColumnSelf = pSelf.columns(joinColumnSelf)
              val pColumnOther = pOther.columns(joinColumnSelf)
              val joinIndex = ts.ComputeJoinIndex.queue(
                left = pColumnSelf,
                right = pColumnOther,
                how = how,
                outputPath = LogicalPath(
                  table = name + ".joinindex",
                  partition = Some(PartitionPath(Nil, numPartitions, pIdx)),
                  0,
                  0
                )
              )

              val joinedPartition
                  : IO[List[(Segment[_], (ColumnDataType, String))]] =
                joinIndex
                  .flatMap {
                    case (takeSelf, takeOther) =>
                      val takenSelf = IO.parSequenceN(32)(
                        (0 until pSelf.columns.size).toList map { cIdx =>
                          val columnInPartition = pSelf.columns(cIdx)
                          ts.BufferColumnAndTakeIndex
                            .queue(
                              columnInPartition,
                              takeSelf,
                              LogicalPath(
                                table = name + ".takeSelf",
                                partition = None,
                                pIdx,
                                cIdx
                              )
                            )
                            .map(
                              (_, (columnInPartition.tpe, self.colNames(cIdx)))
                            )

                        }
                      )
                      val takenOther = IO.parSequenceN(32)(
                        (0 until pOther.columns.size).toList map { cIdx =>
                          val columnInPartition = pOther.columns(cIdx)
                          ts.BufferColumnAndTakeIndex
                            .queue(
                              columnInPartition,
                              takeOther,
                              LogicalPath(
                                table = name + ".takeOther",
                                partition = None,
                                pIdx,
                                cIdx
                              )
                            )
                            .map(
                              (_, (columnInPartition.tpe, other.colNames(cIdx)))
                            )

                        }
                      )

                      IO.both(takenSelf, takenOther)
                        .flatMap {
                          case (takenSelf, takenOther) =>
                            val withoutJoinColumnSelf = takenSelf.zipWithIndex
                              .filterNot(_._2 == joinColumnSelf)
                              .map(_._1)
                            val withoutJoinColumnOther = takenOther.zipWithIndex
                              .filterNot(_._2 == joinColumnOther)
                              .map(_._1)

                            val bond
                                : List[(Segment[_], (ColumnDataType, String))] =
                              withoutJoinColumnSelf ++ withoutJoinColumnOther

                            val mergedJoinColumn
                                : IO[(Segment[_], (ColumnDataType, String))] =
                              how match {
                                case "inner" | "left" =>
                                  IO.pure(
                                    takenSelf.zipWithIndex
                                      .find(_._2 == joinColumnSelf)
                                      .get
                                      ._1
                                  )
                                case "right" =>
                                  IO.pure(
                                    takenOther.zipWithIndex
                                      .find(_._2 == joinColumnOther)
                                      .get
                                      ._1
                                  )
                                case "outer" =>
                                  val (a, (tpeA, nameA)) =
                                    takenSelf.zipWithIndex
                                      .find(_._2 == joinColumnSelf)
                                      .get
                                      ._1
                                  val (b, (tpeB, _)) = takenOther.zipWithIndex
                                    .find(_._2 == joinColumnOther)
                                    .get
                                    ._1

                                  assert(tpeA == tpeB)

                                  ts.MergeNonMissing
                                    .queue(
                                      a,
                                      b,
                                      LogicalPath(
                                        table = name + ".mergejoincolumn",
                                        partition = None,
                                        pIdx,
                                        0
                                      )
                                    )
                                    .map((_, (tpeA, nameA)))

                              }

                            mergedJoinColumn.map {
                              (mergedJoinColumn: (
                                  Segment[_],
                                  (ColumnDataType, String)
                              )) =>
                                List(mergedJoinColumn) ++ bond
                            }

                        }

                  }
              joinedPartition.map { columnsAsSingleSegment =>
                (
                  PartitionedTable(
                    columnsAsSingleSegment.map { case (segment, (tpe, _)) =>
                      Column(Vector(segment), tpe)
                    }.toVector
                  ),
                  columnsAsSingleSegment.map(_._2._2)
                )
              }

          })
        joinedColumnsAndPartitions.map((_, name))
      }
      .map { case (joinedPartitions, name) =>
        assert(joinedPartitions.map(_._2).distinct.size == 1)
        Table(
          joinedPartitions.map(_._1).reduce(_ concatenate _).columns,
          joinedPartitions.headOption
            .map(_._2.toVector)
            .getOrElse(Vector.empty),
          name
        )
      }
  }

  /** Group by which return group locations
    *
    * Returns a triple for each input segment: group map, number of groups,
    * group sizes
    */
  def groupBy(
      cols: Seq[Int],
      numPartitions: Int
  )(implicit
      tsc: TaskSystemComponents
  ): IO[GroupedTable] = {
    assert(
      cols.nonEmpty
    )

    self
      .partition(cols, numPartitions)
      .flatMap { case partitions =>
        val groupedPartitions =
          IO.parSequenceN(32)(partitions.zipWithIndex.map {
            case (partition, pIdx) =>
              val columnsOfGroupBy = cols.map(partition.columns.apply)
              val groupMap = ts.MakeGroupMap.queue(
                columnsOfGroupBy,
                outputPath = LogicalPath(
                  table = self.uniqueId + ".groupmap",
                  partition = Some(PartitionPath(cols, numPartitions, pIdx)),
                  0,
                  0
                )
              )

              groupMap.map { case (a, b, c) => Segment.GroupMap(a, b, c) }

          })
        groupedPartitions
      }
      .map(GroupedTable(self, _))

  }

  /** This is almost noop, concat the list of segments
    *
    * @param others
    * @return
    */
  def concatenate(name: String)(others: Table*): Table = {
    val all = Seq(self) ++ others
    assert(all.map(_.colNames).distinct.size == 1)
    assert(all.map(_.columns.map(_.tpe)).distinct.size == 1)
    val columns = all.head.columns.size
    val cols = 0 until columns map { cIdx =>
      all.map(_.columns(cIdx)).reduce(_ ++ _)
    } toVector

    Table(cols, all.head.colNames, name)
  }

  /** Concat list of columns */
  def addColOfSameSegmentation(c: Column, colName: String)(implicit
      tsc: TaskSystemComponents
  ): IO[Table] = {
    val name = ts.MakeUniqueId.queue(
      self,
      s"addcol-$colName",
      List(c)
    )
    name.map(name =>
      self.copy(
        columns = self.columns.appended(c),
        colNames = self.colNames.appended(colName),
        uniqueId = name
      )
    )
  }

  /** \= Top K selection
    *
    *   - We need an estimate of the CDF
    *   - From the approximate CDF we select the V value below which K elements
    *     fall
    *   - Scan all segments and find the index set which picks those elements
    *     below V . TakeIndex on all columns
    *   - Rearrange into table
    */
  def topK(
      sortColumn: Int,
      ascending: Boolean,
      k: Int,
      cdfCoverage: Double,
      cdfNumberOfSamplesPerSegment: Int
  )(implicit tsc: TaskSystemComponents): IO[Table] = {
    val cdf = self
      .columns(sortColumn)
      .estimateCDF(cdfCoverage, cdfNumberOfSamplesPerSegment)
    val name = ts.MakeUniqueId.queue(
      self,
      s"topk-$sortColumn-$ascending-$k",
      Nil
    )
    val value = cdf.flatMap { cdf =>
      name.flatMap { name =>
        cdf.topK(k, ascending).flatMap {
          case Some(value) =>
            value
              .toSegment(
                LogicalPath(
                  table = name,
                  None,
                  0,
                  0
                )
              )
              .map(Some(_))
          case None => IO.pure(None)
        }
      }
    }

    value.flatMap {
      case None => IO.pure(self)
      case Some(cutoff) =>
        if (ascending) self.rfilterInEquality(sortColumn, cutoff, true)
        else self.rfilterInEquality(sortColumn, cutoff, false)
    }

  }

  /** \== Sorting
    *
    * We sort by parallel distributed sort
    *
    * We sort only on 1 colum
    *
    *   - We need an estimate of the CDF (see doc of other method)
    *   - From the approximate CDF we select n values which partition the data
    *     evenly into n+1 partitions
    *   - We write those partitions (all columns) - Sort the partitions (all
    *     columns)
    *   - Rearrange the sorted partitions in the correct order
    *
    * @param cols
    */
  def sort(
      sortColumn: Int,
      ascending: Boolean
  ): IO[Table] = ???

  /** Pivot is two nested group by followed by aggregation and rearranging the
    * results into a new table
    *
    *   - Get all distinct elements of `columnGroupColumns`. Use group by for
    *     this. This is the new list of columns.
    *
    *   - Partition by columnGroupRows
    *
    *   - Buffer all three columns of a partition, and pivot it in mem. Use the
    *     list of columns, place nulls if needed.
    *   - Concatenate
    *
    * @param columnGroupRows
    * @param columnGroup2
    * @param valueColumn
    * @return
    */
  def pivot(
      columnGroupRows: Int,
      columnGroupColumns: Int,
      valueColumn: Int
  ): IO[Table] = ???

  // Full cartesian joins are infeasible on big data
  // For small data we can use other tools
  // The only use case is small data -> big blowup into big data
  // Best would be to avoid these
  // /** Cartesian product
  //   *
  //   *   - For each segment of this and other
  //   *   - Form cartesian product as streams of buffered tables
  //   */
  // def lazyProduct(
  //     other: Table,
  //     chunkSize: Int = 5000
  // ): Stream[IO, BufferedTable] = ???

  // /**   - Form cartesian product
  //   *   - For each segment of product
  //   *   - Run theta to get predicate
  //   *   - Apply predicate
  //   *   - Write to local segments
  //   *   - Resegment
  //   *
  //   * @param other
  //   * @param chunkSize
  //   * @param theta
  //   */
  // def join(
  //     other: Table,
  //     chunkSize: Int
  // )(theta: BufferedTable => BufferInt) = ???

}

sealed trait ReductionOp {
  def reduce(segment: Segment[_], groupMap: Segment.GroupMap)(implicit
      tsc: TaskSystemComponents
  ): IO[Segment[_]]
  def id: String
}

trait ColumnOps { self: Column =>
  // elementwise operations whose output is a new column of the same size
}

private[ra3] object Utils {
  def writeFully(bb: ByteBuffer, channel: WritableByteChannel) = {
    bb.rewind
    while (bb.hasRemaining) {
      channel.write(bb)
    }
  }
  def readFully(
      bb: ByteBuffer,
      channel: ReadableByteChannel
  ): Unit = {
    bb.clear
    var i = 0
    while (bb.hasRemaining && i >= 0) {
      i = channel.read(bb)
    }
    bb.flip
  }

  def mergeCDFs[T: Ordering](
      cdfs: Seq[Vector[(T, Int)]]
  ): Vector[(T, Int)] = {
    // this is wrong. it should operate on quantiles, not on counts

    val locations = cdfs.flatMap(_.map(_._1)).distinct.sorted
    val ord = implicitly[Ordering[T]]
    val pointsAtLocations = cdfs.map { cdf =>
      locations.map { loc =>
        cdf.find(_._1 == loc) match {
          case None =>
            if (ord.lteq(loc, cdf.head._1)) cdf.head._2
            else if (ord.gteq(loc, cdf.last._1)) cdf.last._2
            else {
              val before = cdf.takeWhile(v => ord.lt(v._1, loc)).last._2
              val after = cdf.dropWhile((v => ord.lt(v._1, loc))).head._2
              val mean = ((after + before) * 0.5).toInt
              mean
            }
          case Some((_, value)) => value
        }
      }
    }
    val aggregated = pointsAtLocations.transpose.map(_.sum)
    assert(aggregated.size == locations.size)
    locations.zip(aggregated).toVector
  }

}
