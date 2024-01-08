package ra3

import cats.effect.IO
import tasks.{TaskSystemComponents}

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
  def take(
      indexes: Column.Int32Column
  )(implicit tsc: TaskSystemComponents): IO[Table] = {
    assert(self.columns.head.segments.size == indexes.segments.size)
    ts.MakeUniqueId.queue(self, "take", List(indexes)).flatMap { name =>
      IO.parTraverseN(math.min(32, self.columns.size))(
        self.columns.zipWithIndex
      ) { case (column, columnIdx) =>
        IO.parTraverseN(math.min(32, indexes.segments.size))(
          indexes.segments.zipWithIndex
        ) { case (segment, segmentIdx) =>
          ts.TakeIndex.queue(
            input = column.segments(segmentIdx),
            idx = segment,
            outputPath = LogicalPath(name, None, segmentIdx, columnIdx)
          )
        }.map { segments => column.tag.makeColumn(segments) }

      }.map(columns =>
        Table(
          columns,
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
      IO.parTraverseN(math.min(32, self.columns.size))(
        self.columns.zipWithIndex
      ) { case (column, columnIdx) =>
        IO.parTraverseN(math.min(32, predicate.segments.size))(
          predicate.segments.zipWithIndex
        ) { case (segment, segmentIdx) =>
          ts.Filter.queue(
            input = column.segments(segmentIdx),
            predicate = segment,
            outputPath = LogicalPath(name, None, segmentIdx, columnIdx)
          )
        }.map(segments => column.tag.makeColumn(segments))

      }.map(columns =>
        Table(
          columns,
          self.colNames,
          name
        )
      )
    }
  }
  def rfilterInEquality(
      columnIdx: Int,
      cutoff: Segment,
      lessThan: Boolean
  )(implicit tsc: TaskSystemComponents): IO[Table] = {
    val comparisonColumn = self.columns(columnIdx)
    val castedCutoff = cutoff.as(comparisonColumn)

    ts.MakeUniqueId
      .queue(
        self,
        "rfilterinequality",
        List(comparisonColumn.tag.makeColumn(Vector(castedCutoff)))
      )
      .flatMap { name =>
        IO.parTraverseN(math.min(32, self.columns.size))(
          self.columns.zipWithIndex
        ) { case (column, columnIdx) =>
          IO.parTraverseN(math.min(32, comparisonColumn.segments.size))(
            comparisonColumn.segments.zipWithIndex
          ) { case (comparisonSegment, segmentIdx) =>
            ts.FilterInequality.queue(
              comparison = comparisonSegment,
              input = column.segments(segmentIdx),
              cutoff = castedCutoff,
              outputPath = LogicalPath(name, None, segmentIdx, columnIdx),
              lessThan = lessThan
            )
          }.map(segments => column.tag.makeColumn(segments))

        }.map(sx =>
          Table(
            sx,
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
              val column = self.columns(columnIdx)
              column.tag.makeColumn(segments.map(_.as(column)))
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
      self.columns(joinColumnSelf).tag == other
        .columns(joinColumnOther)
        .tag
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
              val pColumnOther = pOther
                .columns(joinColumnSelf)
                .asInstanceOf[pColumnSelf.ColumnType]

              val joinIndex = ts.ComputeJoinIndex.queue(
                left = pColumnSelf.asInstanceOf[pColumnSelf.ColumnType],
                right = pColumnOther,
                how = how,
                outputPath = LogicalPath(
                  table = name + ".joinindex",
                  partition = Some(PartitionPath(Nil, numPartitions, pIdx)),
                  0,
                  0
                )
              )

              val joinedPartition =
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
                              (
                                _,
                                (
                                  columnInPartition.tag,
                                  self.colNames(cIdx)
                                )
                              )
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
                              (
                                _,
                                (
                                  columnInPartition.tag,
                                  other.colNames(cIdx)
                                )
                              )
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

                            val bond =
                              withoutJoinColumnSelf ++ withoutJoinColumnOther

                            val mergedJoinColumn =
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
                                  val (a, (tpeA: ColumnTag, nameA)) =
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
                                    .queue(tpeA)(
                                      a.as(tpeA),
                                      b.as(tpeA),
                                      LogicalPath(
                                        table = name + ".mergejoincolumn",
                                        partition = None,
                                        pIdx,
                                        0
                                      )
                                    )
                                    .map((_, (tpeA, nameA)))

                              }

                            mergedJoinColumn.map { mergedJoinColumn =>
                              List(mergedJoinColumn) ++ bond
                            }

                        }

                  }
              joinedPartition.map { columnsAsSingleSegment =>
                (
                  PartitionedTable(
                    columnsAsSingleSegment.map { case (segment, (tpe, _)) =>
                      tpe.makeColumn(Vector(segment.as(tpe)))
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

    val name = ts.MakeUniqueId.queue(
      self,
      s"groupby-${cols.mkString("_")}-$numPartitions",
      Nil
    )
    name.flatMap { name =>
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

                groupMap.map { case (a, b, c) =>
                  (partition, Segment.GroupMap(a, b, c))
                }

            })
          groupedPartitions
        }
        .map(GroupedTable(_, this.colNames, name))
    }

  }

  /** This is almost noop, select columns
    *
    * @param others
    * @return
    */
  def selectColumns(
      columnIndexes: Int*
  )(implicit tsc: TaskSystemComponents): IO[Table] = {
    val name = ts.MakeUniqueId.queue(
      self,
      s"select-columns-${columnIndexes.mkString("_")}",
      Nil
    )
    name.map { name =>
      val cols = columnIndexes map { cIdx =>
        this.columns(cIdx)
      } toVector

      Table(cols, colNames, name)
    }
  }

  def filterColumnNames(nameSuffix: String)(p: String => Boolean) = {

    val keep = columns.zip(colNames).filter(v => p(v._2))
    Table(keep.map(_._1), keep.map(_._2), uniqueId + nameSuffix)
  }

  /** This is almost noop, concat the list of segments
    *
    * @param others
    * @return
    */
  def concatenate(
      others: Table*
  )(implicit tsc: TaskSystemComponents): IO[Table] = {
    val name = ts.MakeUniqueId.queue(
      self,
      s"concat",
      this.columns ++ others.flatMap(_.columns)
    )
    name.map { name =>
      val all = Seq(self) ++ others
      assert(all.map(_.colNames).distinct.size == 1)
      assert(all.map(_.columns.map(_.tag)).distinct.size == 1)
      val columns = all.head.columns.size
      val cols = 0 until columns map { cIdx =>
        all.map(_.columns(cIdx)).reduce(_ castAndConcatenate _)
      } toVector

      Table(cols, all.head.colNames, name)
    }
  }

  /** Concat list of columns */
  def addColOfSameSegmentation(c: Column, colName: String)(implicit
      tsc: TaskSystemComponents
  ): IO[Table] = {
    assert(c.segments.map(_.numElems) == this.segmentation)
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

trait ReductionOp {
  def reduce[S <: Segment { type SegmentType = S }](
      segment: Seq[Segment],
      groupMap: Segment.GroupMap,
      path: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ): IO[S]
  def id: String
}

trait ColumnOps { self: Column =>
  // elementwise operations whose output is a new column of the same size
}
