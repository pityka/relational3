package ra3

case class PartitionedTable(columns: Vector[Column]) {
  def concatenate(other: PartitionedTable) = {
    assert(columns.size == other.columns.size)
    assert(columns.map(_.tag) == other.columns.map(_.tag))
    PartitionedTable(
      columns.zip(other.columns).map { case (a, b) => a castAndConcatenate b }
    )
  }
}