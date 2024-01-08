package ra3

import cats.effect.IO
import tasks.{TaskSystemComponents}

case class GroupedTable(
    partitions: Seq[(PartitionedTable, Segment.GroupMap)],
    colNames: Vector[String],
    uniqueId: String
) {
  assert(partitions.map(_._1.numCols).distinct.size == 1)
  assert(colNames.size == partitions.head._1.numCols)

  def extractGroups(implicit tsc: TaskSystemComponents): IO[Seq[Table]] = {

    val name = ts.MakeUniqueId.queue0(
      s"$uniqueId-extractgroups",
      List()
    )

    assert(partitions.map(_._1.columns.map(_.tag)).distinct.size == 1)
    val columns = partitions.head._1.columns.size

    name.flatMap { name =>
      IO.parSequenceN(math.min(32, partitions.size))(
        partitions.map { case (partition, groupMap) =>
          IO.parSequenceN(math.min(32, columns))(
            (0 until columns).toVector.map { columnIdx =>
              val segments = partition.columns(columnIdx).segments
              val tag = partition.columns(columnIdx).tag

              assert(
                segments.map(_.numElems).sum == groupMap.map.numElems
              )

              val groupsFromSegments = ts.ExtractGroups.queue[tag.SegmentType](
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
                .map {
                  //   groups
                  groups =>
                    groups.map { segmentOfGroup =>
                      tag.makeColumn(Vector(segmentOfGroup))

                    }
                }
            }
          ).map {
            // columns x groups
            columns =>
              columns.transpose.map(columns => PartitionedTable(columns))
          }

        }
      ).map(_.flatten)
        .map { groups =>
          // groups x columns
          groups.zipWithIndex.map { case (partitionedTable, gIdx) =>
            Table(partitionedTable.columns, this.colNames, name + "-g" + gIdx)
          }
        }
    }

  }

  def reduceGroups(reductions: Seq[ReductionOp])(implicit
      tsc: TaskSystemComponents
  ) = {
    assert(this.colNames.size == reductions.size)
    val columns = colNames.size
    val name = ts.MakeUniqueId.queue0(
      s"$uniqueId-reduce-${reductions.map(_.id).mkString("-")}",
      List()
    )
    name.flatMap { name =>
      IO.parSequenceN(math.min(32, partitions.size))(
        partitions.zipWithIndex.map {
          case ((partition, groupMap), partitionIdx) =>
            IO.parSequenceN(math.min(32, columns))(
              (0 until columns).toVector.map { columnIdx =>
                val segments = partition.columns(columnIdx).segments
                val tag = partition.columns(columnIdx).tag

                assert(
                  segments.map(_.numElems).sum == groupMap.map.numElems
                )

                val reducedSegment =
                  reductions(columnIdx).reduce[tag.SegmentType](
                    segments,
                    groupMap,
                    LogicalPath(
                      table = name,
                      partition = None,
                      segment = partitionIdx, // partitions will become segments
                      column = columnIdx
                    )
                  )

                reducedSegment
                  .map {
                    //   groups
                    reducedSegment =>
                      tag.makeColumn(Vector(reducedSegment))

                  }
              }
            ).map {
              // columns
              columns =>
                PartitionedTable(columns)
            }

        }
      ).map { partitions =>
        Table(partitions.reduce(_ concatenate _).columns, this.colNames, name)
      }
    }

  }
}
