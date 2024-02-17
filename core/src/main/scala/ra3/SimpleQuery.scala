package ra3

import tasks.TaskSystemComponents
import cats.effect.IO

private[ra3] object SimpleQuery {
  def simpleQuery(self: Table, program: ra3.lang.Query)(implicit
      tsc: TaskSystemComponents
  ) = {
    val nSegments = self.columns.head.segments.size
    ts.MakeUniqueId
      .queue(self, "where-" + program.hash, Nil)
      .flatMap { name =>
        IO.parTraverseN(math.min(32, nSegments))(
          (0 until nSegments).toList
        ) { segmentIdx =>
          ts.SimpleQuery.queue(
            input = self.columns.map(_.segments(segmentIdx)).zipWithIndex.map {
              case (s, columnIdx) =>
                ra3.ts.SegmentWithName(
                  segment = List(s),
                  tableUniqueId = self.uniqueId,
                  columnName = self.colNames(columnIdx),
                  columnIdx = columnIdx
                )
            },
            predicate = program,
            outputPath = LogicalPath(name, None, segmentIdx, 0),
            groupMap = None
          )
        }.map(segments =>
          segments.transpose.map { case segments =>
            val tag = segments.head._1.tag
            val col =
              tag.makeColumn(segments.map(_._1).toVector.map(_.as(tag)))
            val name = segments.head._2
            (col, name)
          }
        ).map(columns =>
          Table(
            columns.map(_._1).toVector,
            columns.map(_._2).toVector,
            name,
            self.partitions
          )
        )
      }
  }
  def simpleQueryCount(self: Table, program: ra3.lang.Query)(implicit
      tsc: TaskSystemComponents
  ): IO[Table] = {
    val nSegments = self.columns.head.segments.size
    ts.MakeUniqueId
      .queue(self, "simple-count-" + program.hash, Nil)
      .flatMap { name =>
        IO.parTraverseN(math.min(32, nSegments))(
          (0 until nSegments).toList
        ) { segmentIdx =>
          ts.SimpleQueryCount.queue(
            input = self.columns.map(_.segments(segmentIdx)).zipWithIndex.map {
              case (s, columnIdx) =>
                ra3.ts.SegmentWithName(
                  segment = List(s),
                  tableUniqueId = self.uniqueId,
                  columnName = self.colNames(columnIdx),
                  columnIdx = columnIdx
                )
            },
            predicate = program,
            groupMap = None
          )
        }.map(_.map(_.toLong).foldLeft(0L)(_ + _))
          .flatMap { count =>
            ra3.ColumnTag.I64
              .makeColumnFromSeq(name, 0)(List(List(count)))
              .map { column =>
                Table(
                  columns = Vector(column),
                  colNames = Vector("count"),
                  uniqueId = name,
                  partitions = None
                )
              }
          }
      }
  }
}
