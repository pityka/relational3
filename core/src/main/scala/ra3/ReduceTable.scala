package ra3

import tasks.TaskSystemComponents

object ReduceTable {
  def formSingleGroupAsOnePartitionPerSegment(
      self: Table
  )(implicit tsc: TaskSystemComponents) = {
    val name = ts.MakeUniqueId.queue(
      self,
      s"reduceTable",
      Nil
    )
    name.map { name =>
      GroupedTable(
        PartitionedTable.makeFakePartitionsFromEachSegment(self).map {
          partitionedTable =>
            (
              partitionedTable,
              Segment.GroupMap(
                map = SegmentInt(
                  None,
                  partitionedTable.numRows.toInt,
                  statistic =
                    StatisticInt.constant(0)
                ),
                numGroups = 1,
                groupSizes = SegmentInt(
                  None,
                  1,
                  StatisticInt.constant(partitionedTable.numRows.toInt)
                )
              )
            )
        },
        self.colNames,
        name
      )
    }
  }
  def formSingleGroup(self: Table)(implicit tsc: TaskSystemComponents) = {
    require(
      self.numRows < Int.MaxValue - 100,
      "Too long table, can't reduce it at once"
    )
    val name = ts.MakeUniqueId.queue(
      self,
      s"reduceTable",
      Nil
    )
    name.map { name =>
      val singleGroup = GroupedTable(
        List(
          (
            PartitionedTable(
              columns = self.columns,
              partitionMeta = PartitionMeta(Nil, 0)
            ),
            Segment.GroupMap(
              map = SegmentInt(
                None,
                self.numRows.toInt,
                statistic = StatisticInt.constant(0)
              ),
              numGroups = 1,
              groupSizes = SegmentInt(
                None,
                1,
                StatisticInt.constant(self.numRows.toInt)
              )
            )
          )
        ),
        self.colNames,
        name
      )
      singleGroup
    }
  }

}
