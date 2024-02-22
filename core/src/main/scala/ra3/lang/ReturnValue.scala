package ra3.lang

import ra3._

private[ra3] sealed trait ColumnSpec
private[ra3] sealed trait ColumnSpecWithValue[T] extends ColumnSpec {
  def value: T
}
private[ra3] sealed trait NamedColumnSpec[T] extends ColumnSpecWithValue[T] {
  def name: String
}
private[ra3] sealed trait UnnamedColumnSpec[T] extends ColumnSpecWithValue[T] {
  def withName(s: String): NamedColumnSpec[T]
}
private[ra3] case class NamedColumnChunk(
    value: Either[Buffer, Seq[Segment]],
    name: String
) extends NamedColumnSpec[Either[Buffer, Seq[Segment]]]
private[ra3] case class NamedConstantI32(value: Int, name: String)
    extends NamedColumnSpec[Int]
private[ra3] case class NamedConstantI64(value: Long, name: String)
    extends NamedColumnSpec[Long]
private[ra3] case class NamedConstantString(value: String, name: String)
    extends NamedColumnSpec[String]
private[ra3] case class NamedConstantF64(value: Double, name: String)
    extends NamedColumnSpec[Double]
private[ra3] case class UnnamedColumnChunk(value: Either[Buffer, Seq[Segment]])
    extends UnnamedColumnSpec[Either[Buffer, Seq[Segment]]] {
  def withName(s: String): NamedColumnChunk = NamedColumnChunk(value, s)
}
private[ra3] case class UnnamedConstantI32(value: Int)
    extends UnnamedColumnSpec[Int] {
  def withName(s: String): NamedColumnSpec[Int] = NamedConstantI32(value, s)
}
private[ra3] case class UnnamedConstantI64(value: Long)
    extends UnnamedColumnSpec[Long] {
  def withName(s: String): NamedColumnSpec[Long] = NamedConstantI64(value, s)
}
private[ra3] case class UnnamedConstantString(value: String)
    extends UnnamedColumnSpec[String] {
  def withName(s: String): NamedColumnSpec[String] =
    NamedConstantString(value, s)
}
private[ra3] case class UnnamedConstantF64(value: Double)
    extends UnnamedColumnSpec[Double] {
  def withName(s: String): NamedColumnSpec[Double] = NamedConstantF64(value, s)
}
private[ra3] case object Star extends ColumnSpec

private[ra3] case class ReturnValue(
    projections: List[ColumnSpec],
    filter: Option[DI32]
)
