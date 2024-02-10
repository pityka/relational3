package ra3

import cats.effect.IO
import tasks.TaskSystemComponents

case class PartitionMeta(
    columns: Seq[Int],
    partitionBase: Int
) {
  def prefix(p: Seq[Int]) = {
    assert(columns.startsWith(p))
    PartitionMeta(p, partitionBase)
  }
  def numPartitions =
    (1 to columns.size).foldLeft(1)((acc, _) => acc * partitionBase)
  def partitionIdxOfPrefix(
      prefix: Seq[Int],
      partitionMapOverSegments: Vector[Int]
  ): Vector[Int] = {
    assert(columns.startsWith(prefix))
    val div = (1 to (columns.size - prefix.size)).foldLeft(1)((acc, _) =>
      acc * partitionBase
    )
    partitionMapOverSegments.map { p1 =>
      p1 / div
    }
  }
  def isPrefixOf(other: PartitionMeta) =
    other.partitionBase == this.partitionBase && other.columns.startsWith(
      columns
    )
}

case class PartitionedTable(
    columns: Vector[Column],
    partitionMeta: PartitionMeta
) {
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
    assert(other.partitionMeta == this.partitionMeta)
    assert(columns.size == other.columns.size)
    assert(columns.map(_.tag) == other.columns.map(_.tag))
    PartitionedTable(
      columns.zip(other.columns).map { case (a, b) => a castAndConcatenate b },
      partitionMeta
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

object PartitionedTable {

  def makeFakePartitionsFromEachSegment(self: Table): Seq[PartitionedTable] =
    (0 until self.columns.head.segments.size).toVector map { segmentIdx =>
      PartitionedTable(
        self.columns.map(col =>
          col.tag.makeColumn(Vector(col.segments(segmentIdx)))
        ),
        PartitionMeta(Nil, 1)
      )
    }
  def partitionColumns(
      columnIdx: Seq[Int],
      inputColumns: Vector[Column],
      partitionBase: Int,
      uniqueId: String,
      maxSegmentsToBufferAtOnce: Int
  )(implicit tsc: TaskSystemComponents): IO[Vector[PartitionedTable]] = {
    assert(columnIdx.nonEmpty)
    val partitionColumns = columnIdx.map(inputColumns.apply)
    val numSegments = partitionColumns.head.segments.size
    val segmentIdxs = (0 until numSegments).toVector
    val numPartitions =
      (1 to columnIdx.size).foldLeft(1)((acc, _) => acc * partitionBase)
    IO.parTraverseN(math.min(32, numSegments))(segmentIdxs) { segmentIdx =>
      val segmentsOfP = partitionColumns.map(_.segments(segmentIdx)).toVector
      val partitionMap = ts.MakePartitionMap.queue(
        input = segmentsOfP,
        partitionBase = partitionBase,
        outputPath = LogicalPath(
          table = uniqueId + "-partitionmap",
          partition = Some(PartitionPath(columnIdx, numPartitions, 0)),
          segment = segmentIdx,
          column = 0
        )
      )
      partitionMap
    }.flatMap { partitionMapsPerSegment =>
      val segmentIndices = (0 until numSegments).toList
      val groups: List[(List[(Int, SegmentInt)], Int)] = segmentIndices
        .zip(partitionMapsPerSegment)
        .grouped(maxSegmentsToBufferAtOnce)
        .toList
        .zipWithIndex

      // columns x partition x groupOfSegments
      val partitionsOfColumns =
        IO.parTraverseN(math.min(32, inputColumns.size))(
          inputColumns.zipWithIndex
        ) { case (column, currentColumnIdx) =>
          IO.parSequenceN(32)(groups.map { case (groupOfSegments, groupIdx) =>
            val segmentsWithPartitionmapOfThisColumn
                : Seq[(column.SegmentType, SegmentInt)] =
              groupOfSegments.map { case (segmentIdx, partitionMap) =>
                (column.segments(segmentIdx), partitionMap)
              }

            ts.TakePartition.queue(
              inputSegmentsWithPartitionMaps =
                segmentsWithPartitionmapOfThisColumn,
              numPartition = numPartitions,
              outputPath = LogicalPath(
                table = uniqueId,
                partition = Some(PartitionPath(columnIdx, numPartitions, -1)),
                segment = groupIdx,
                column = currentColumnIdx
              )
            )

          }).map {
            // group x partitions
            partitionsOfGroupsOfThisColumn =>
              assert(partitionsOfGroupsOfThisColumn.size == groups.size)
              // partition x groupOfSegments
              val tp = partitionsOfGroupsOfThisColumn.transpose 
              assert(tp.size == numPartitions)
              // partition
              tp
          }

        }

      partitionsOfColumns.map {
        // column x partition  x groupOfSegment
        columns =>
          assert(columns.size == inputColumns.size)
          assert(columns.forall(x => x.size == numPartitions))
          // partition x column 
          val transposed = columns.transpose
          
        assert(transposed.size == numPartitions)
          transposed.map { columns =>
            PartitionedTable(
              columns = columns.zipWithIndex.map { case (segments, columnIdx) =>
                val column = inputColumns(columnIdx)
                column.tag.makeColumn(segments.toVector.map(_.as(column)))
              },
              partitionMeta = PartitionMeta(
                columns = columnIdx,
                partitionBase = partitionBase
              )
            )
          }
      }
    }
  }
}
