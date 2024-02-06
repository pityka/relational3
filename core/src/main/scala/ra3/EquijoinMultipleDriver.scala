package ra3
import cats.effect.IO
import tasks.TaskSystemComponents
object EquijoinMultipleDriver {
  def equijoinMultiple(
    self: Table,
      joinColumnSelf: Int,
      others: Seq[(Table, Int, String, Int)],
      partitionBase: Int,
      partitionLimit: Int,
  program : ra3.lang.Expr {
    type T <: ra3.lang.ReturnValue
  })(implicit tsc: TaskSystemComponents) = {
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
      "This join of 3+ queries would need repartitioning. Use the other paiwise equijoin method or better plan the joins"
    )

    val pSelf =
      if (bufferSelf)
        Vector(PartitionedTable(self.columns, PartitionMeta(Nil, 1)))
      else
        (0 until self.columns.head.segments.size).toVector map { segmentIdx =>
          PartitionedTable(
            self.columns.map(col =>
              col.tag.makeColumn(Vector(col.segments(segmentIdx)))
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
              col.tag.makeColumn(Vector(col.segments(segmentIdx)))
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

              val joinIndex = ts.ComputeJoinIndex.queue(
                first = pFirst.columns(joinColumnSelf),
                rest = pRest.zip(others).map {
                  case (p, (_, joinColIdx, how, joinTarget)) =>
                    (p.columns(joinColIdx), how, joinTarget)
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
                  .flatMap { case takes =>
                    ts.MultipleTableQuery.queue(
                      input = partitionOfTablesWithTables.flatMap {
                        case (partitionedTable, table) =>
                          partitionedTable.columns.zipWithIndex.map {
                            case (s, columnIdx) =>
                              ra3.ts.SegmentWithName(
                                segment = s.segments,
                                tableUniqueId = table.uniqueId,
                                columnName = table.colNames(columnIdx),
                                columnIdx = columnIdx
                              )
                          }
                      },
                      predicate = program,
                      outputPath = LogicalPath(name, None, pIdx, 0),
                      takes = (List(self) ++ others.map(_._1)).zip(takes).map {
                        case (table, take) => (table.uniqueId, take)
                      }
                    )

                  }
              joinedPartition.map { columnsAsSingleSegment =>
                (
                  TableHelper(
                    columnsAsSingleSegment.map { case (segment, _) =>
                      segment.tag.makeColumn(Vector(segment.asSegmentType))
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
          joinedPartitions.map(_._1).reduce(_ concatenate _).columns,
          joinedPartitions.headOption
            .map(_._2.toVector)
            .getOrElse(Vector.empty),
          name,
          None
        )
      }
  }
}