package ra3


import cats.effect.IO
import tasks.{TaskSystemComponents}



case class GroupedTable(
    table: Table,
    groups: Seq[Segment.GroupMap]
) {

  def extractGroups(implicit tsc: TaskSystemComponents): IO[Seq[Table]] = {

    val name = ts.MakeUniqueId.queue(
      this.table,
      s"extractgroups",
      List(ColumnTag.I32.makeColumn(this.groups.map(_.map).toVector))
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
                  groupMap.map,
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
                column.tag.makeColumn(segments)

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
      List(ColumnTag.I32.makeColumn(this.groups.map(_.map).toVector))
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
          ).map(segments => column.tag.makeColumn(segments))
        }
      ).map { columns =>
        Table(columns, this.table.colNames, name)
      }
    }
  }
}