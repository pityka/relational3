package ra3

private[ra3] case class PartitionPath(
    partitionedOnColumns: Seq[Int],
    numPartitions: Int,
    partitionId: Int
)

private[ra3] case class LogicalPath(
    table: String,
    partition: Option[PartitionPath],
    segment: Int,
    column: Int
) {
  def appendToTable(suffix: String) = copy(table = table + suffix)
  override def toString = {
    val part = partition
      .map {
        case PartitionPath(by, pnum, pidx) if by.nonEmpty =>
          s"/partitions/by-${by.mkString("-")}/totalparts-$pnum/pidx-$pidx"
        case PartitionPath(_, pnum, pidx) =>
          s"/partitions/total-$pnum/pidx-$pidx"
      }
      .getOrElse("")
    s"$table$part/segments/$segment/columns/$column"
  }
}
