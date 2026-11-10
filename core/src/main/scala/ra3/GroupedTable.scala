package ra3

import cats.effect.IO
import tasks.ParentTask
import tasks.ExtendedTaskSystemComponents
import tasks.tsParent
import tasks.jsonitersupport.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*

private[ra3] case class GroupedTable(
    partitions: Seq[(PartitionedTable, Segment.GroupMap)],
    colNames: Vector[String],
    uniqueId: String
) {
  assert(partitions.map(_._1.numCols).distinct.size == 1)
  assert(colNames.size == partitions.head._1.numCols)

  def extractGroups(implicit
      tsc: ExtendedTaskSystemComponents
  ): IO[Seq[Table]] =
    GroupedTable.extractGroupsTask(this)

}

private[ra3] object GroupedTable {

  // $COVERAGE-OFF$
  implicit val codec: JsonValueCodec[GroupedTable] = JsonCodecMaker.make
  implicit val codecSeqTable: JsonValueCodec[Seq[Table]] = JsonCodecMaker.make
  // $COVERAGE-ON$

  private[ra3] case class ReduceInput(
      self: GroupedTable,
      program: ra3.lang.runtime.Expr
  )
  private[ra3] object ReduceInput {
    implicit val codec: JsonValueCodec[ReduceInput] = JsonCodecMaker.make
  }

  private[ra3] case class CountInput(
      self: GroupedTable,
      program: ra3.lang.runtime.Expr
  )
  private[ra3] object CountInput {
    implicit val codec: JsonValueCodec[CountInput] = JsonCodecMaker.make
  }

  private def extractGroupsImpl(self: GroupedTable)(implicit
      tsc: ExtendedTaskSystemComponents
  ): IO[Seq[Table]] = {
    val name = ts.MakeUniqueId.queue0(
      s"${self.uniqueId}-extractgroups",
      List()
    )

    assert(self.partitions.map(_._1.columns.map(_.tag)).distinct.size == 1)
    val columns = self.partitions.head._1.columns.size

    name.flatMap { name =>
      IO.parSequenceN(math.min(32, self.partitions.size))(
        self.partitions.map { case (partition, groupMap) =>
          IO.parSequenceN(math.min(32, columns))(
            (0 until columns).toVector.map { columnIdx =>
              val column = partition.columns(columnIdx)
              val tag = column.tag
              val segments = tag.segments(column.column)

              assert(
                segments.map(_.numElems).sum == groupMap.map.numElems
              )

              val groupsFromSegments = ts.ExtractGroups.queue(tag)(
                segments,
                groupMap.map,
                groupMap.numGroups,
                LogicalPath(
                  table = name,
                  partition = None,
                  segment = 0,
                  column = columnIdx
                )
              )

              groupsFromSegments
                .map { groups =>
                  groups.map { segmentOfGroup =>
                    tag.makeTaggedColumn(
                      tag.makeColumn(Vector(segmentOfGroup))
                    )

                  }
                }
            }
          ).map { columns =>
            // columns x groups
            columns.transpose.map(columns => TableHelper(columns))
          }

        }
      ).map(_.flatten)
        .map { groups =>
          // groups x columns
          groups.zipWithIndex.map { case (partitionedTable, gIdx) =>
            Table(
              partitionedTable.columns,
              self.colNames,
              name + "-g" + gIdx,
              None
            )
          }
        }
    }
  }

  private def reduceGroupsImpl(
      self: GroupedTable,
      program: ra3.lang.runtime.Expr
  )(implicit
      tsc: ExtendedTaskSystemComponents
  ): IO[Table] = {
    val columns = self.colNames.size
    val name = ts.MakeUniqueId.queue0(
      s"${self.uniqueId}-reduce-${program.hash}",
      List()
    )
    name.flatMap { name =>
      IO.parSequenceN(math.min(32, self.partitions.size))(
        self.partitions.zipWithIndex.map {
          case ((partition, groupMap), partitionIdx) =>
            ts.SimpleQuery
              .queue(
                input = (0 until columns).toVector.map { columnIdx =>
                  val col = partition
                    .columns(columnIdx)
                  val segments = col.tag.segments(col.column)
                  ra3.ts.TypedSegmentWithName(
                    tag = col.tag,
                    segment = segments,
                    tableUniqueId = self.uniqueId,
                    columnName = self.colNames(columnIdx),
                    columnIdx = columnIdx
                  )
                },
                predicate = program,
                outputPath = LogicalPath(name, None, partitionIdx, 0),
                groupMap = Option((groupMap.map, groupMap.numGroups))
              )
              .map(columnSegments =>
                columnSegments.map { case segmentOfColumn =>
                  val col: TaggedColumn =
                    segmentOfColumn._1.tag.makeTaggedColumn(
                      segmentOfColumn._1.tag
                        .makeColumn(Vector(segmentOfColumn._1.segment))
                    )
                  val name = segmentOfColumn._2
                  (col, name)
                }
              )
              .map(columns =>
                (
                  TableHelper(columns.map(_._1).toVector),
                  columns.map(_._2)
                )
              )

        }
      ).map { partitions =>
        Table(
          partitions.map(_._1).reduce(_ `concatenate` _).columns,
          partitions.head._2.toVector,
          name,
          None
        )
      }
    }
  }

  private def countGroupsImpl(
      self: GroupedTable,
      program: ra3.lang.runtime.Expr
  )(implicit
      tsc: ExtendedTaskSystemComponents
  ): IO[Table] = {
    val columns = self.colNames.size
    val name = ts.MakeUniqueId.queue0(
      s"${self.uniqueId}-count-groups-${program.hash}",
      List()
    )
    name.flatMap { name =>
      IO.parSequenceN(math.min(32, self.partitions.size))(
        self.partitions.zipWithIndex.map { case ((partition, groupMap), _) =>
          ts.SimpleQueryCount
            .queue(
              input = (0 until columns).toVector.map { columnIdx =>
                partition
                  .columns(columnIdx)
                  .tag -> ra3.ts.SegmentWithName(
                  segment = partition
                    .columns(columnIdx)
                    .segments,
                  tableUniqueId = self.uniqueId,
                  columnName = self.colNames(columnIdx),
                  columnIdx = columnIdx
                )
              },
              predicate = program,
              groupMap = Option((groupMap.map, groupMap.numGroups))
            )

        }
      ).map(_.map(_.toLong).foldLeft(0L)(_ + _))
        .flatMap { count =>
          ra3.ColumnTag.I64
            .makeColumnFromSeq(name, 0)(List(List(count)))
            .map { column =>
              Table(
                columns = Vector(ra3.ColumnTag.I64.makeTaggedColumn(column)),
                colNames = Vector("count"),
                uniqueId = name,
                partitions = None
              )
            }
        }
    }.logElapsed
  }

  val extractGroupsTask =
    ParentTask[GroupedTable, Seq[Table]]("groupedExtractGroups", 1) { self =>
      implicit cxt => extractGroupsImpl(self)
    }

  val reduceGroupsTask =
    ParentTask[ReduceInput, Table]("groupedReduceGroups", 1) { input =>
      implicit cxt => reduceGroupsImpl(input.self, input.program)
    }

  val countGroupsTask =
    ParentTask[CountInput, Table]("groupedCountGroups", 1) { input =>
      implicit cxt => countGroupsImpl(input.self, input.program)
    }

  def reduceGroups(
      self: GroupedTable,
      program: ra3.lang.runtime.Expr
  )(implicit
      tsc: ExtendedTaskSystemComponents
  ): IO[Table] =
    reduceGroupsTask(ReduceInput(self, program))

  def countGroups(
      self: GroupedTable,
      program: ra3.lang.runtime.Expr
  )(implicit
      tsc: ExtendedTaskSystemComponents
  ): IO[Table] =
    countGroupsTask(CountInput(self, program))
}
