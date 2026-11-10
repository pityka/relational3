package ra3

import cats.effect.IO
import tasks.{TaskSystemComponents, ExtendedTaskSystemComponents}
import tasks.ParentTask
import tasks.tsParent
import tasks.SharedFile
import tasks.jsonitersupport.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import ra3.ts.ExportCsv
private[ra3] trait RelationalAlgebra { self: Table =>

  private[ra3] def rfilterInEquality(
      columnIdx: Int,
      cutoff: Segment,
      lessThan: Boolean
  )(implicit tsc: ExtendedTaskSystemComponents): IO[Table] =
    RelationalAlgebra.rfilterTask(
      RelationalAlgebra.RFilterInput(self, columnIdx, cutoff, lessThan)
    )

  private[ra3] def prePartition(
      columnIdx: Seq[Int],
      partitionBase: Int,
      partitionLimit: Int,
      maxItemsToBufferAtOnce: Int
  )(implicit tsc: ExtendedTaskSystemComponents) = {
    partition(
      columnIdx = columnIdx,
      partitionBase = partitionBase,
      numPartitionsIsImportant = true,
      partitionLimit = partitionLimit,
      maxItemsToBufferAtOnce = maxItemsToBufferAtOnce
    ).map { parts =>
      val pz = parts.zipWithIndex
      val partitionMapOverSegments =
        pz.flatMap(v => v._1.segmentation.map(_ => v._2))
      val cat = parts.reduce(_ `concatenate` _)

      Table(
        cat.columns,
        self.colNames,
        self.uniqueId + "partitioned-" + columnIdx.mkString(
          "-"
        ) + "-" + partitionBase + "-" + partitionLimit,
        if (parts.size == 1) None
        else
          Some(
            PartitionData(
              columns = columnIdx,
              partitionBase = partitionBase,
              partitionMapOverSegments = partitionMapOverSegments.toVector
            )
          )
      )

    }.logElapsed

  }

  private[ra3] def partition(
      columnIdx: Seq[Int],
      partitionBase: Int,
      numPartitionsIsImportant: Boolean,
      partitionLimit: Int,
      maxItemsToBufferAtOnce: Int
  )(implicit tsc: ExtendedTaskSystemComponents): IO[Vector[PartitionedTable]] =
    (if (self.numRows <= partitionLimit.toLong)
       IO.pure(Vector(PartitionedTable(self.columns, PartitionMeta(Nil, 1))))
     else if (
       self.partitions.isDefined && self.partitions.get.columns == columnIdx && (self.partitions.get.partitionBase == partitionBase || !numPartitionsIsImportant)
     )
       IO {
         (0 until self.partitions.get.numPartitions).toVector.map { pIdx =>
           val segmentIdx =
             self.partitions.get.partitionMapOverSegments.zipWithIndex
               .filter(_._1 == pIdx)
               .map(_._2)
           val columns = self.columns.map { col =>
             col.tag.makeTaggedColumn(
               col.tag.makeColumn(segmentIdx.map(col.segments))
             )
           }
           PartitionedTable(columns, PartitionMeta(columnIdx, partitionBase))
         }
       }
     else if (
       self.partitions.isDefined && self.partitions.get.columns.startsWith(
         columnIdx
       ) && (self.partitions.get.partitionBase == partitionBase)
     ) IO {
       val pmeta = PartitionMeta(self.partitions.get.columns, partitionBase)
       val pmetaPrefix = pmeta.prefix(columnIdx)
       val num = pmetaPrefix.numPartitions
       val partitionMap = pmeta.partitionIdxOfPrefix(
         columnIdx,
         self.partitions.get.partitionMapOverSegments
       )
       (0 until num).toVector.map { pIdx =>
         val segmentIdx =
           partitionMap.zipWithIndex
             .filter(_._1 == pIdx)
             .map(_._2)
         val columns = self.columns.map { col =>
           col.tag.makeTaggedColumn(
             col.tag.makeColumn(segmentIdx.map(col.segments))
           )
         }
         PartitionedTable(columns, pmetaPrefix)
       }
     }
     else {
       scribe.info(
         f"Will partition ${self.columns.size} columns of ${self.uniqueId} by partition key of column $columnIdx, each ${self.segmentation.size}%,d segments, total elements: ${self.numRows}%,d. "
       )
       PartitionedTable.partitionColumns(
         columnIdx = columnIdx,
         inputColumns = self.columns,
         partitionBase = partitionBase,
         maxItemsToBufferAtOnce = maxItemsToBufferAtOnce,
         uniqueId = self.uniqueId + "partitioned-" + columnIdx.mkString(
           "-"
         ) + "-" + partitionBase + "-" + partitionLimit
       )
     }).logElapsed

  /** Group by which return group locations
    *
    * Returns a triple for each input segment: group map, number of groups,
    * group sizes
    */
  private[ra3] def groupBy(
      cols: Seq[Int],
      partitionBase: Int,
      partitionLimit: Int,
      maxItemsToBufferAtOnce: Int
  )(implicit
      tsc: ExtendedTaskSystemComponents
  ): IO[GroupedTable] = {
    assert(
      cols.nonEmpty
    )

    val name = ts.MakeUniqueId.queue(
      self,
      s"groupby-${cols.mkString("_")}-$partitionBase",
      Nil
    )
    name.flatMap { name =>
      self
        .partition(
          columnIdx = cols,
          partitionBase = partitionBase,
          numPartitionsIsImportant = false,
          partitionLimit = partitionLimit,
          maxItemsToBufferAtOnce = maxItemsToBufferAtOnce
        )
        .flatMap { case partitions =>
          scribe.info(
            f"Partitioning of $name done with ${partitions.size} partitions with sizes min=${partitions
                .map(_.numRows)
                .min}%,2d max=${partitions.map(_.numRows).max}%,2d. Will find groups of each partition."
          )
          val groupedPartitions =
            IO.parSequenceN(32)(partitions.zipWithIndex.map {
              case (partition, pIdx) =>
                val columnsOfGroupBy = cols.map(partition.columns.apply)
                val groupMap = ts.MakeGroupMap.queue(
                  columnsOfGroupBy,
                  outputPath = LogicalPath(
                    table = self.uniqueId + ".groupmap",
                    partition =
                      Some(PartitionPath(cols, partitions.size, pIdx)),
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
    }.logElapsed

  }

  /** Group by without partitioning
    *
    * Useful to reduce the segments without partitioning
    */
  private[ra3] def groupBySegments(
      cols: Seq[Int]
  )(implicit
      tsc: ExtendedTaskSystemComponents
  ): IO[GroupedTable] = {
    assert(
      cols.nonEmpty
    )

    val name = ts.MakeUniqueId.queue(
      self,
      s"groupbysegments-${cols.mkString("_")}",
      Nil
    )
    name.flatMap { name =>
      val partitions = PartitionedTable.makeFakePartitionsFromEachSegment(self)

      IO.parSequenceN(32)(partitions.zipWithIndex.map {
        case (partition, pIdx) =>
          val columnsOfGroupBy = cols.map(partition.columns.apply)
          val groupMap = ts.MakeGroupMap.queue(
            columnsOfGroupBy,
            outputPath = LogicalPath(
              table = self.uniqueId + ".groupmap",
              partition = Some(PartitionPath(cols, partitions.size, pIdx)),
              0,
              0
            )
          )

          groupMap.map { case (a, b, c) =>
            (partition, Segment.GroupMap(a, b, c))
          }

      }).map(GroupedTable(_, this.colNames, name))
    }.logElapsed

  }

  /** This is almost noop, concat the list of segments
    *
    * @param others
    * @return
    */
  private[ra3] def concatenate(
      others: Table*
  )(implicit tsc: ExtendedTaskSystemComponents): IO[Table] = {
    val name = ts.MakeUniqueId.queue(
      self,
      s"concat",
      (this.columns ++ others.flatMap(_.columns)).map(_.column)
    )
    name.map { name =>
      val all = Seq(self) ++ others
      assert(all.map(_.colNames).distinct.size == 1)
      assert(all.map(_.columns.map(_.tag)).distinct.size == 1)
      val columns = all.head.columns.size
      val cols = 0 until columns map { cIdx =>
        all.map(_.columns(cIdx)).reduce(_ `castAndConcatenate` _)
      } toVector

      Table(cols, all.head.colNames, name, None)
    }.logElapsed
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
  private[ra3] def topK(
      sortColumn: Int,
      ascending: Boolean,
      k: Int,
      cdfCoverage: Double,
      cdfNumberOfSamplesPerSegment: Int
  )(implicit tsc: ExtendedTaskSystemComponents): IO[Table] = {
    val cdf = {
      val col = self
        .columns(sortColumn)
      Column
        .estimateCDF(col.tag)(
          col.column,
          cdfCoverage,
          cdfNumberOfSamplesPerSegment
        )
    }

    val name = ts.MakeUniqueId.queue(
      self,
      s"topk-$sortColumn-$ascending-$k",
      Nil
    )
    val value = cdf.flatMap { cdf =>
      name.flatMap { name =>
        val perc = k / numRows.toDouble

        cdf.topK(perc, ascending).flatMap {
          case Some(value) =>
            ra3.ts.MakeSegmentFromBuffer
              .queue(
                value,
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
    }.logElapsed

  }

  def exportToCsv(
      columnSeparator: Char = ',',
      quoteChar: Char = '"',
      recordSeparator: String = "\r\n",
      compression: Option[ra3.ts.ExportCsv.CompressionFormat] = Some(
        ExportCsv.Gzip
      )
  )(implicit tsc: ExtendedTaskSystemComponents) = {
    RelationalAlgebra.exportCsvTask(
      RelationalAlgebra.ExportCsvInput(
        self,
        columnSeparator,
        quoteChar,
        recordSeparator,
        compression
      )
    )

  }

  def exportToParquet(
      snappy: Boolean
  )(implicit tsc: ExtendedTaskSystemComponents) = {
    RelationalAlgebra.exportParquetTask(
      RelationalAlgebra.ExportParquetInput(self, snappy)
    )

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
  private def sort(
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
  private def pivot(
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

private[ra3] object RelationalAlgebra {

  private[ra3] case class RFilterInput(
      self: Table,
      columnIdx: Int,
      cutoff: Segment,
      lessThan: Boolean
  )
  private[ra3] object RFilterInput {
    implicit val codec: JsonValueCodec[RFilterInput] = JsonCodecMaker.make
  }

  private[ra3] case class ExportCsvInput(
      self: Table,
      columnSeparator: Char,
      quoteChar: Char,
      recordSeparator: String,
      compression: Option[ExportCsv.CompressionFormat]
  )
  private[ra3] object ExportCsvInput {
    implicit val codec: JsonValueCodec[ExportCsvInput] = JsonCodecMaker.make
  }

  private[ra3] case class ExportParquetInput(
      self: Table,
      snappy: Boolean
  )
  private[ra3] object ExportParquetInput {
    implicit val codec: JsonValueCodec[ExportParquetInput] = JsonCodecMaker.make
  }

  // $COVERAGE-OFF$
  implicit val codecSeqSharedFile: JsonValueCodec[Seq[SharedFile]] =
    JsonCodecMaker.make
  // $COVERAGE-ON$

  private[ra3] def rfilterImpl(
      input: RFilterInput
  )(implicit tsc: ExtendedTaskSystemComponents): IO[Table] = {
    val self = input.self
    val lessThan = input.lessThan
    val comparisonColumn = self.columns(input.columnIdx)

    val castedCutoff =
      input.cutoff.asInstanceOf[comparisonColumn.tag.SegmentType]

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
            val inputSegment = column.segments(segmentIdx)
            ts.FilterInequality.queue(
              tag = column.tag,
              comparisonTag = comparisonColumn.tag
            )(
              comparison = comparisonSegment: comparisonColumn.tag.SegmentType,
              input = inputSegment,
              cutoff = castedCutoff,
              outputPath = LogicalPath(name, None, segmentIdx, columnIdx),
              lessThan = lessThan
            )
          }.map(segments =>
            column.tag
              .makeTaggedColumn(column.tag.makeColumn(segments.toVector))
          )

        }.map(sx =>
          Table(
            sx,
            self.colNames,
            name,
            self.partitions
          )
        )
      }
      .logElapsed
  }

  private[ra3] def exportCsvImpl(
      input: ExportCsvInput
  )(implicit tsc: ExtendedTaskSystemComponents): IO[Seq[SharedFile]] = {
    val self = input.self
    IO.parSequenceN(32)((0 until self.segmentation.size).toList map {
      segmentIdx =>
        val cols = self.columns.map(tc =>
          tc.tag.makeTaggedSegment(tc.tag.segments(tc.column)(segmentIdx))
        )
        ts.ExportCsv.queue(
          segments = cols,
          columnSeparator = input.columnSeparator,
          quoteChar = input.quoteChar,
          recordSeparator = input.recordSeparator,
          outputName = self.uniqueId,
          outputSegmentIndex = segmentIdx,
          compression = input.compression
        )
    }).logElapsed
  }

  private[ra3] def exportParquetImpl(
      input: ExportParquetInput
  )(implicit tsc: ExtendedTaskSystemComponents): IO[Seq[SharedFile]] = {
    val self = input.self
    IO.parSequenceN(32)((0 until self.segmentation.size).toList map {
      segmentIdx =>
        val cols = self.columns.map(tc =>
          tc.tag.makeTaggedSegment(tc.tag.segments(tc.column)(segmentIdx))
        )
        ts.ExportParquet.queue(
          segments = cols,
          colNames = self.colNames,
          outputName = self.uniqueId,
          outputSegmentIndex = segmentIdx,
          snappy = input.snappy
        )
    }).logElapsed
  }

  val rfilterTask =
    ParentTask[RFilterInput, Table]("rfilterInEquality", 1) { input =>
      implicit cxt => rfilterImpl(input)
    }

  val exportCsvTask =
    ParentTask[ExportCsvInput, Seq[SharedFile]]("exportToCsv", 1) { input =>
      implicit cxt => exportCsvImpl(input)
    }

  val exportParquetTask =
    ParentTask[ExportParquetInput, Seq[SharedFile]]("exportToParquet", 1) {
      input => implicit cxt => exportParquetImpl(input)
    }

}
