package ra3.lang

import ra3._

sealed trait ColumnSpec
sealed trait ColumnSpecWithValue[T] extends ColumnSpec {
  def value: T
}
sealed trait NamedColumnSpec[T] extends ColumnSpecWithValue[T] {
  def name: String
}
sealed trait UnnamedColumnSpec[T] extends ColumnSpecWithValue[T] {
  def withName(s: String): NamedColumnSpec[T]
}
case class NamedColumnChunk(value: Either[Buffer, Seq[Segment]], name: String)
    extends NamedColumnSpec[Either[Buffer, Seq[Segment]]]
case class NamedConstantI32(value: Int, name: String)
    extends NamedColumnSpec[Int]
case class NamedConstantI64(value: Long, name: String)
    extends NamedColumnSpec[Long]
case class NamedConstantString(value: String, name: String)
    extends NamedColumnSpec[String]
case class NamedConstantF64(value: Double, name: String)
    extends NamedColumnSpec[Double]
case class UnnamedColumnChunk(value: Either[Buffer, Seq[Segment]])
    extends UnnamedColumnSpec[Either[Buffer, Seq[Segment]]] {
  def withName(s: String): NamedColumnChunk = NamedColumnChunk(value, s)
}
case class UnnamedConstantI32(value: Int) extends UnnamedColumnSpec[Int] {
  def withName(s: String): NamedColumnSpec[Int] = NamedConstantI32(value, s)
}
case class UnnamedConstantI64(value: Long) extends UnnamedColumnSpec[Long] {
  def withName(s: String): NamedColumnSpec[Long] = NamedConstantI64(value, s)
}
case class UnnamedConstantString(value: String)
    extends UnnamedColumnSpec[String] {
  def withName(s: String): NamedColumnSpec[String] =
    NamedConstantString(value, s)
}
case class UnnamedConstantF64(value: Double) extends UnnamedColumnSpec[Double] {
  def withName(s: String): NamedColumnSpec[Double] = NamedConstantF64(value, s)
}
case object Star extends ColumnSpec

case class ReturnValue(
    projections: List[ColumnSpec],
    filter: Option[DI32]
)
