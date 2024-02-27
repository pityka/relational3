package ra3

import cats.effect.IO
import tasks.{TaskSystemComponents}
import ra3.ts.ExportCsv
import tablelang._
import lang._
private[ra3] trait RelationalAlgebra { self: Table =>

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
          name,
          self.partitions
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
          name,
          self.partitions
        )
      )
    }
  }
  def in0(
      body: ra3.tablelang.TableExpr.Ident => ra3.tablelang.TableExpr
  ): ra3.tablelang.TableExpr =
    ra3.lang.local(ra3.tablelang.TableExpr.Const(this))(body)

  def in[T0 <: Expr.DelayedIdent: NotNothing](
      body: (T0) => TableExpr
  ): TableExpr = {
    local(TableExpr.Const(self)) { t =>
        body(t[T0](0))
    }
  }
  def in[T0<: Expr.DelayedIdent: NotNothing, T1<: Expr.DelayedIdent: NotNothing](
      body: (
          T0,
          T1
      ) => TableExpr
  ): TableExpr = {
    local(TableExpr.Const(self)) { t =>
      t.apply[T0, T1](0, 1) { case (c0, c1) =>
        body(c0, c1)
      }
    }
  }
  def in[T0<: Expr.DelayedIdent: NotNothing, T1<: Expr.DelayedIdent: NotNothing, T2<: Expr.DelayedIdent: NotNothing](
      body: (
          T0,
          T1,
          T2
      ) => TableExpr
  ): TableExpr = {
    local(TableExpr.Const(self)) { t =>
      t.apply[T0, T1, T2](0, 1, 2) { case (c0, c1, c2) =>
        body(c0, c1, c2)
      }
    }
  }
  def in[T0<: Expr.DelayedIdent: NotNothing, T1<: Expr.DelayedIdent: NotNothing, T2<: Expr.DelayedIdent: NotNothing, T3<: Expr.DelayedIdent: NotNothing](
      body: (
          T0,
          T1,
          T2,
          T3
      ) => TableExpr
  ): TableExpr = {
    local(TableExpr.Const(self)) { t =>
      t.apply[T0, T1, T2, T3](0, 1, 2, 3) { case (c0, c1, c2, c3) =>
        body(c0, c1, c2, c3)
      }
    }
  }
  def in[
      T0<: Expr.DelayedIdent: NotNothing,
      T1<: Expr.DelayedIdent: NotNothing,
      T2<: Expr.DelayedIdent: NotNothing,
      T3<: Expr.DelayedIdent: NotNothing,
      T4<: Expr.DelayedIdent: NotNothing
  ](
      body: (
          T0,
          T1,
          T2,
          T3,
          T4
      ) => TableExpr
  ): TableExpr = {
    local(TableExpr.Const(self)) { t =>
      t.apply[T0, T1, T2, T3, T4](0, 1, 2, 3, 4) {
        case (c0, c1, c2, c3, c4) =>
          body(c0, c1, c2, c3, c4)
      }
    }
  }
  def in[
      T0<: Expr.DelayedIdent: NotNothing,
      T1<: Expr.DelayedIdent: NotNothing,
      T2<: Expr.DelayedIdent: NotNothing,
      T3<: Expr.DelayedIdent: NotNothing,
      T4<: Expr.DelayedIdent: NotNothing,
      T5<: Expr.DelayedIdent: NotNothing,
  ](
      body: (
          T0,
          T1,
          T2,
          T3,
          T4,
          T5,
      ) => TableExpr
  ): TableExpr = {
    local(TableExpr.Const(self)) { t =>
      t.apply[T0, T1, T2, T3, T4,T5](0, 1, 2, 3, 4, 5) {
        case (c0, c1, c2, c3, c4, c5) =>
          body(c0, c1, c2, c3, c4, c5)
      }
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
            name,
            self.partitions
          )
        )
      }
  }

  def prePartition(
      columnIdx: Seq[Int],
      partitionBase: Int,
      partitionLimit: Int,
      maxSegmentsToBufferAtOnce: Int
  )(implicit tsc: TaskSystemComponents) = {
    partition(
      columnIdx = columnIdx,
      partitionBase = partitionBase,
      numPartitionsIsImportant = true,
      partitionLimit = partitionLimit,
      maxSegmentsToBufferAtOnce = maxSegmentsToBufferAtOnce
    ).map { parts =>
      val pz = parts.zipWithIndex
      val partitionMapOverSegments =
        pz.flatMap(v => v._1.segmentation.map(_ => v._2))
      val cat = parts.reduce(_ concatenate _)

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

    }

  }

  def partition(
      columnIdx: Seq[Int],
      partitionBase: Int,
      numPartitionsIsImportant: Boolean,
      partitionLimit: Int,
      maxSegmentsToBufferAtOnce: Int
  )(implicit tsc: TaskSystemComponents): IO[Vector[PartitionedTable]] =
    if (self.numRows <= partitionLimit.toLong)
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
            col.tag.makeColumn(segmentIdx.map(col.segments))
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
          col.tag.makeColumn(segmentIdx.map(col.segments))
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
        maxSegmentsToBufferAtOnce = maxSegmentsToBufferAtOnce,
        uniqueId = self.uniqueId + "partitioned-" + columnIdx.mkString(
          "-"
        ) + "-" + partitionBase + "-" + partitionLimit
      )
    }

  /** Group by which return group locations
    *
    * Returns a triple for each input segment: group map, number of groups,
    * group sizes
    */
  def groupBy(
      cols: Seq[Int],
      partitionBase: Int,
      partitionLimit: Int,
      maxSegmentsToBufferAtOnce: Int
  )(implicit
      tsc: TaskSystemComponents
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
          maxSegmentsToBufferAtOnce = maxSegmentsToBufferAtOnce
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
    }

  }

  /** Group by without partitioning
    *
    * Useful to reduce the segments without partitioning
    */
  def groupBySegments(
      cols: Seq[Int]
  )(implicit
      tsc: TaskSystemComponents
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

      Table(
        cols,
        columnIndexes.map(colNames).toVector,
        name,
        None // could keep it, but the column index would shift away. Need stable column identifier
      )
    }
  }

  def filterColumnNames(nameSuffix: String)(p: String => Boolean) = {

    val keep = columns.zip(colNames).filter(v => p(v._2))
    Table(
      keep.map(_._1),
      keep.map(_._2),
      uniqueId + nameSuffix,
      None // see comment in previous method
    )
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

      Table(cols, all.head.colNames, name, None)
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
        val perc = k / numRows.toDouble

        cdf.topK(perc, ascending).flatMap {
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

  def exportToCsv(
      columnSeparator: Char = ',',
      quoteChar: Char = '"',
      recordSeparator: String = "\r\n",
      compression: Option[ra3.ts.ExportCsv.CompressionFormat] = Some(
        ExportCsv.Gzip
      )
  )(implicit tsc: TaskSystemComponents) = {
    IO.parSequenceN(32)((0 until self.segmentation.size).toList map {
      segmentIdx =>
        val cols = self.columns.map(_.segments(segmentIdx))
        ts.ExportCsv.queue(
          segments = cols,
          columnSeparator = columnSeparator,
          quoteChar = quoteChar,
          recordSeparator = recordSeparator,
          outputName = self.uniqueId,
          outputSegmentIndex = segmentIdx,
          compression = compression
        )
    })

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
