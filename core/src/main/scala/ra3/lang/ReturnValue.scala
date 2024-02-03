package ra3.lang

import ra3.BufferInt

sealed trait ColumnSpec
case class ColumnName[T](a: T, b: String) extends ColumnSpec
case class UnnamedColumn[T](a: T) extends ColumnSpec
case object Star extends ColumnSpec


case class ReturnValue(
  projections: List[ColumnSpec],
  filter: Option[BufferInt]
)