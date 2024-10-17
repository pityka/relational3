package ra3
import cats.effect.IO
import tasks.TaskSystemComponents
import ra3.lang.ReturnValueTuple
private[ra3] object Equijoin {

  def equijoinPlanner(
      self: Table,
      joinColumnSelf: Int,
      others: Seq[(Table, Int, String, Int)],
      partitionBase: Int,
      partitionLimit: Int,
      maxSegmentsToBufferAtOnce: Int,
      program: ra3.lang.runtime.Expr
  )(implicit tsc: TaskSystemComponents): IO[Table] = {
    if (others.size == 0) {
      scribe.info("Nothing to join")
      IO.pure(self)
    } else if (others.size == 1) {
      scribe.info("Do pairwise join")
      equijoinTwo(
        self = self,
        other = others.head._1,
        joinColumnSelf = joinColumnSelf,
        joinColumnOther = others.head._2,
        how = others.head._3,
        partitionBase = partitionBase,
        partitionLimit = partitionLimit,
        maxSegmentsToBufferAtOnce = maxSegmentsToBufferAtOnce,
        program
      )
    } else {
      val bufferSelf = self.numRows <= partitionLimit.toLong
      val bufferOthers = others.map(_._1.numRows <= partitionLimit)
      val numberOfTablesWhichDontFit =
        (List(bufferSelf) ++ bufferOthers).count(b => !b)
      if (numberOfTablesWhichDontFit <= 1) {
        scribe.info("Do  join of 3+ tables, all but one fits in memory")
        equijoinMultiple(
          self,
          joinColumnSelf,
          others,
          partitionBase,
          partitionLimit,
          program
        )
      } else {
        val numberOfTablesWhichFit =
          (List(bufferSelf) ++ bufferOthers).count(identity)
        if (numberOfTablesWhichFit == 0) {
          require(
            false,
            "Tertiary or higher joins all requiring repartitioning are not supported. Explicitly break them up into pairwise joins."
          )
          ???
        } else {
          ???
          // generate all possible associated formulations
          // order by the number of partitionings
          // if the minimum number of partitionings is 3 then fail
          // in the outermost join use the supplied program
          // in the rest use a select(star)
          // in the supplied program replace table identifiers

        }
      }
    }
  }

  def equijoinTwo(
      self: Table,
      other: Table,
      joinColumnSelf: Int,
      joinColumnOther: Int,
      how: String,
      partitionBase: Int,
      partitionLimit: Int,
      maxSegmentsToBufferAtOnce: Int,
      program: ra3.lang.runtime.Expr
  )(implicit tsc: TaskSystemComponents) = {

    assert(
      self.columns(joinColumnSelf).tag == other
        .columns(joinColumnOther)
        .tag
    )

    val name = ts.MakeUniqueId.queue2(
      self,
      other,
      s"join-$how-$partitionBase-$joinColumnSelf-$joinColumnOther-${program.hash}",
      Nil
    )

    // if either one fits in, then we scan the other
    val noPartitioning =
      (self.numRows <= partitionLimit.toLong || other.numRows <= partitionLimit.toLong)
    val bufferSelf = self.numRows <= partitionLimit.toLong
    val bufferOther = other.numRows <= partitionLimit.toLong

    val pSelf = if (noPartitioning) {
      if (bufferSelf)
        IO.pure(Vector(PartitionedTable(self.columns, PartitionMeta(Nil, 1))))
      else
        IO.pure {
          PartitionedTable.makeFakePartitionsFromEachSegment(self)
        }
    } else
      self.partition(
        columnIdx = List(joinColumnSelf),
        partitionBase = partitionBase,
        numPartitionsIsImportant = true,
        partitionLimit = partitionLimit,
        maxSegmentsToBufferAtOnce = maxSegmentsToBufferAtOnce
      )

    val pOther = if (noPartitioning) {
      if (bufferOther)
        IO.pure(Vector(PartitionedTable(other.columns, PartitionMeta(Nil, 1))))
      else
        IO.pure {
          PartitionedTable.makeFakePartitionsFromEachSegment(other)
        }
    } else
      other.partition(
        columnIdx = List(joinColumnOther),
        partitionBase = partitionBase,
        numPartitionsIsImportant = true,
        partitionLimit = partitionLimit,
        maxSegmentsToBufferAtOnce = maxSegmentsToBufferAtOnce
      )
    IO.both(name, IO.both(pSelf, pOther))
      .flatMap { case (name, (pSelf, pOther)) =>
        assert(pSelf.size == pOther.size || pSelf.size == 1 || pOther.size == 1)
        val zippedPartitions =
          if (pSelf.size > 1 && pOther.size > 1) (pSelf zip pOther)
          else if (pSelf.size == 1) pOther.map(o => (pSelf.head, o))
          else pSelf.map(s => (s, pOther.head))

        zippedPartitions.foreach { case (a, b) =>
          scribe.info(
            f"Joining partitions of sizes: ${a.numRows}%,d x ${b.numRows}%,d "
          )
        }

        val joinedColumnsAndPartitions =
          IO.parSequenceN(32)(zippedPartitions.zipWithIndex.map {
            case ((pSelf, pOther), pIdx) =>
              val pColumnSelf = pSelf.columns(joinColumnSelf)
              val joinColumnTag = pColumnSelf.tag
              val pColumnOther = pOther
                .columns(joinColumnOther)
                .column
                .asInstanceOf[joinColumnTag.ColumnType]

              val joinIndex = ts.ComputeJoinIndex
                .queue(joinColumnTag)(
                  first = pColumnSelf.column,
                  rest = List((pColumnOther, how, 0)),
                  outputPath = LogicalPath(
                    table = name + ".joinindex",
                    partition =
                      Some(PartitionPath(Nil, zippedPartitions.size, pIdx)),
                    segment = 0,
                    column = 0
                  )
                )
                .map { list => (list.head, list.last) }

              val joinedPartition =
                joinIndex
                  .flatMap { case (takeSelf, takeOther) =>
                    ts.MultipleTableQuery.queue(
                      input = pSelf.columns.zipWithIndex.map {
                        case (taggedColumn, columnIdx) =>
                          val tag = taggedColumn.tag
                          ra3.ts.TypedSegmentWithName(
                            tag = tag,
                            segment = tag.segments(taggedColumn.column),
                            tableUniqueId = self.uniqueId,
                            columnName = self.colNames(columnIdx),
                            columnIdx = columnIdx
                          )
                      } ++ pOther.columns.zipWithIndex.map {
                        case (taggedColumn, columnIdx) =>
                          val tag = taggedColumn.tag
                          ra3.ts.TypedSegmentWithName(
                            tag = tag,
                            segment = tag.segments(taggedColumn.column),
                            tableUniqueId = other.uniqueId,
                            columnName = other.colNames(columnIdx),
                            columnIdx = columnIdx
                          )
                      },
                      predicate = program,
                      outputPath = LogicalPath(name, None, pIdx, 0),
                      takes = List(
                        (self.uniqueId -> takeSelf),
                        (other.uniqueId -> takeOther)
                      )
                    )

                  }
              joinedPartition.map { columnsAsSingleSegment =>
                (
                  TableHelper(
                    columnsAsSingleSegment.map { case (taggedSegment, _) =>
                      val tag = taggedSegment.tag
                      val segment = taggedSegment.segment
                      tag.makeTaggedColumn(
                        tag.makeColumn(
                          Vector(segment)
                        )
                      )
                    }.toVector
                  ),
                  columnsAsSingleSegment.map(_._2) // colnames
                )
              }

          })
        joinedColumnsAndPartitions.map((_, name))
      }
      .map { case (joinedPartitions, name) =>
        assert(joinedPartitions.map(_._2).distinct.size == 1)
        Table(
          joinedPartitions.map(_._1).reduce(_ `concatenate` _).columns,
          joinedPartitions.headOption
            .map(_._2.toVector)
            .getOrElse(Vector.empty),
          name,
          None
        )
      }
  }

  def equijoinMultiple(
      self: Table,
      joinColumnSelf: Int,
      others: Seq[(Table, Int, String, Int)],
      partitionBase: Int,
      partitionLimit: Int,
      program: ra3.lang.runtime.Expr
  )(implicit tsc: TaskSystemComponents) = {
    val name = ts.MakeUniqueId.queueM(
      self +: others.map(_._1),
      s"join-$partitionBase-$joinColumnSelf-${others
          .map(_._2)
          .mkString("-")}-${others.map(_._3).mkString("-")}-${program.hash}",
      Nil
    )

    // if either one fits in, then we scan the other
    val bufferSelf = self.numRows <= partitionLimit.toLong
    val bufferOthers = others.map(_._1.numRows <= partitionLimit)
    val numberOfTablesWhichDontFit =
      (List(bufferSelf) ++ bufferOthers).count(b => !b)
    require(
      numberOfTablesWhichDontFit <= 1,
      "This join of 3+ queries would need repartitioning. Use the other pairwise equijoin method or better plan the joins"
    )

    val pSelf =
      if (bufferSelf)
        Vector(PartitionedTable(self.columns, PartitionMeta(Nil, 1)))
      else
        (0 until self.columns.head.segments.size).toVector map { segmentIdx =>
          PartitionedTable(
            self.columns.map(col =>
              col.tag.makeTaggedColumn(
                col.tag.makeColumn(Vector(col.segments(segmentIdx)))
              )
            ),
            PartitionMeta(Nil, 1)
          )
        }

    val pOthers = others.map { case (other, _, _, _) =>
      if (other.numRows <= partitionLimit)
        Vector(PartitionedTable(other.columns, PartitionMeta(Nil, 1)))
      else
        (0 until other.columns.head.segments.size).toVector map { segmentIdx =>
          PartitionedTable(
            other.columns.map(col =>
              col.tag.makeTaggedColumn(
                col.tag.makeColumn(Vector(col.segments(segmentIdx)))
              )
            ),
            PartitionMeta(Nil, 1)
          )
        }

    }
    name
      .flatMap { name =>
        val partitions = pSelf +: pOthers
        assert(partitions.count(_.size > 1) <= 1)
        val idxOfTableWhichIsScanned =
          partitions.zipWithIndex.find(_._1.size > 1).map(_._2)
        val zippedPartitions = idxOfTableWhichIsScanned match {
          case None => partitions.transpose
          case Some(i) =>
            partitions(i).map { p =>
              partitions.zipWithIndex.map { case (ps, j) =>
                if (i == j) p
                else ps.head
              }
            }
        }

        val joinedColumnsAndPartitions =
          IO.parSequenceN(32)(zippedPartitions.zipWithIndex.map {
            case (partitionOfTables, pIdx) =>
              val pFirst = partitionOfTables.head
              val pRest = partitionOfTables.tail

              val partitionOfTablesWithTables =
                partitionOfTables zip (List(self) ++ others.map(_._1))

              val pFirstJoinTaggedColumn = pFirst.columns(joinColumnSelf)

              val joinIndex =
                ts.ComputeJoinIndex.queue(pFirstJoinTaggedColumn.tag)(
                  first = pFirstJoinTaggedColumn.column,
                  rest = pRest.zip(others).map {
                    case (p, (_, joinColIdx, how, joinTarget)) =>
                      (
                        p.columns(joinColIdx)
                          .column
                          .asInstanceOf[pFirstJoinTaggedColumn.tag.ColumnType],
                        how,
                        joinTarget
                      )
                  },
                  outputPath = LogicalPath(
                    table = name + ".joinindex",
                    partition =
                      Some(PartitionPath(Nil, zippedPartitions.size, pIdx)),
                    segment = 0,
                    column = 0
                  )
                )

              val joinedPartition =
                joinIndex
                  .flatMap {
                    case takes =>
                      ts.MultipleTableQuery.queue(
                        input =
                          partitionOfTablesWithTables
                            .flatMap { case (partitionedTable, table) =>
                              partitionedTable.columns.zipWithIndex.map {
                                case (taggedColumn, columnIdx) =>
                                  val tag = taggedColumn.tag
                                  ra3.ts.TypedSegmentWithName(
                                    tag = tag,
                                    segment = tag.segments(taggedColumn.column),
                                    tableUniqueId = table.uniqueId,
                                    columnName = table.colNames(columnIdx),
                                    columnIdx = columnIdx
                                  )
                              }
                            },
                        predicate = program,
                        outputPath = LogicalPath(name, None, pIdx, 0),
                        takes =
                          (List(self) ++ others.map(_._1)).zip(takes).map {
                            case (table, take) => (table.uniqueId, take)
                          }
                      )

                  }
              joinedPartition.map { columnsAsSingleSegment =>
                (
                  TableHelper(
                    columnsAsSingleSegment.map { case (taggedSegment, _) =>
                      val tag = taggedSegment.tag
                      val segment = taggedSegment.segment
                      tag.makeTaggedColumn(
                        tag.makeColumn(
                          Vector(segment)
                        )
                      )
                    }.toVector
                  ),
                  columnsAsSingleSegment.map(_._2) // colnames
                )
              }

          })
        joinedColumnsAndPartitions.map((_, name))
      }
      .map { case (joinedPartitions, name) =>
        assert(joinedPartitions.map(_._2).distinct.size == 1)
        Table(
          joinedPartitions.map(_._1).reduce(_ `concatenate` _).columns,
          joinedPartitions.headOption
            .map(_._2.toVector)
            .getOrElse(Vector.empty),
          name,
          None
        )
      }
  }
}
