package ra3

case class PartitionPath(
    partitionedOnColumns: Seq[Int],
    numPartitions: Int,
    partitionId: Int
)

case class LogicalPath(
    table: String,
    partition: Option[PartitionPath],
    segment: Int,
    column: Int
) {
  def appendToTable(suffix: String) = copy(table = table + suffix)
  override def toString = {
    val part = partition.map {
      case PartitionPath(by, pnum, pidx) if by.nonEmpty =>
        s"/partitions/${by.mkString("-")}/$pnum/$pidx"
      case PartitionPath(_, pnum, pidx) => s"/partitions//$pnum/$pidx"
    }.getOrElse("")
    s"$table$part/segments/$segment/columns/$column"
  }
}