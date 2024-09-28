package ra3.lang

import ra3.*

private[ra3] sealed trait ColumnSpec[+T]

private[ra3] sealed trait ColumnSpecWithValue[+T] extends ColumnSpec[T] {
  def value: T
}

private[ra3] sealed trait NamedColumnSpec[T] extends ColumnSpecWithValue[T] {
  def name: String
}

// Columns

private[ra3] sealed trait ColumnSpecWithColumnChunkValue[T]
    extends ColumnSpecWithValue[T]    
    

object NamedColumnSpecWithColumnChunkValueExtractor {
  def unapply(
      t: NamedColumnSpecWithColumnChunkValue[?]
  ) : Option[(Either[TaggedBuffer,TaggedSegments],String)]= {
    t match
      case NamedColumnChunkI32(value, name) => 
          Some((value.map(ColumnTag.I32.makeTaggedSegments).left.map(ColumnTag.I32.makeTaggedBuffer),name))
      case NamedColumnChunkI64(value, name) =>
        Some((value.map(ColumnTag.I64.makeTaggedSegments).left.map(ColumnTag.I64.makeTaggedBuffer),name))
      case NamedColumnChunkF64(value, name) =>
        Some((value.map(ColumnTag.F64.makeTaggedSegments).left.map(ColumnTag.F64.makeTaggedBuffer),name))
      case NamedColumnChunkStr(value, name) =>
        Some((value.map(ColumnTag.StringTag.makeTaggedSegments).left.map(ColumnTag.StringTag.makeTaggedBuffer),name))
      case NamedColumnChunkInst(value, name) =>
        Some((value.map(ColumnTag.Instant.makeTaggedSegments).left.map(ColumnTag.Instant.makeTaggedBuffer),name))
    
    
  }
}

private[ra3] sealed trait NamedColumnSpecWithColumnChunkValue[T]
    extends ColumnSpecWithColumnChunkValue[T]
    with NamedColumnSpec[T] {

  def name: String

}
private[ra3] sealed trait UnnamedColumnSpecWithColumnChunkValue[T]
    extends ColumnSpecWithColumnChunkValue[T] {

  def withName(s: String): NamedColumnSpecWithColumnChunkValue[T]

}

private[ra3] final case class NamedColumnChunkI32(
    value: DI32,
    name: String
) extends NamedColumnSpecWithColumnChunkValue[DI32]
  with ColumnSpec[DI32]
private[ra3] final case class NamedColumnChunkI64(
    value: DI64,
    name: String
) extends NamedColumnSpecWithColumnChunkValue[DI64]
with ColumnSpec[DI64]

private[ra3] final case class NamedColumnChunkF64(
    value: DF64,
    name: String
) extends NamedColumnSpecWithColumnChunkValue[DF64]
with ColumnSpec[DF64]

private[ra3] final case class NamedColumnChunkStr(
    value: DStr,
    name: String
) extends NamedColumnSpecWithColumnChunkValue[DStr]
with ColumnSpec[DStr]

private[ra3] final case class NamedColumnChunkInst(
    value: DInst,
    name: String
) extends NamedColumnSpecWithColumnChunkValue[DInst]
with ColumnSpec[DInst]

private[ra3] final case class UnnamedColumnChunkI32(value: DI32)
    extends UnnamedColumnSpecWithColumnChunkValue[DI32]
    with ColumnSpec[DI32]
    {
  def withName(s: String) = NamedColumnChunkI32(value, s)
}

private[ra3] final case class UnnamedColumnChunkI64(value: DI64)
    extends UnnamedColumnSpecWithColumnChunkValue[DI64]
    with ColumnSpec[DI64]
     {
  def withName(s: String) = NamedColumnChunkI64(value, s)
}

private[ra3] final case class UnnamedColumnChunkF64(value: DF64)
    extends UnnamedColumnSpecWithColumnChunkValue[DF64]
    with ColumnSpec[DF64]
     {
  def withName(s: String) = NamedColumnChunkF64(value, s)
}
private[ra3] final case class UnnamedColumnChunkStr(value: DStr)
    extends UnnamedColumnSpecWithColumnChunkValue[DStr]
    with ColumnSpec[DStr]
    {
  def withName(s: String) = NamedColumnChunkStr(value, s)
}

private[ra3] final case class UnnamedColumnChunkInst(value: DInst)
    extends UnnamedColumnSpecWithColumnChunkValue[DInst]
    with ColumnSpec[DInst]
     {
  def withName(s: String) = NamedColumnChunkInst(value, s)
}

// Constants


private[ra3] sealed trait UnnamedColumnSpec[T] extends ColumnSpecWithValue[T] {
  def withName(s: String): NamedColumnSpec[T]

}

private[ra3] final case class NamedConstantI32(value: Int, name: String)
    extends NamedColumnSpec[Int]
private[ra3] final case class NamedConstantI64(value: Long, name: String)
    extends NamedColumnSpec[Long]
private[ra3] final case class NamedConstantString(value: String, name: String)
    extends NamedColumnSpec[String]
private[ra3] final case class NamedConstantF64(value: Double, name: String)
    extends NamedColumnSpec[Double]

private[ra3] final case class UnnamedConstantI32(value: Int)
    extends UnnamedColumnSpec[Int] {
  def withName(s: String): NamedColumnSpec[Int] = NamedConstantI32(value, s)
}
private[ra3] final case class UnnamedConstantI64(value: Long)
    extends UnnamedColumnSpec[Long] {
  def withName(s: String): NamedColumnSpec[Long] = NamedConstantI64(value, s)
}
private[ra3] final case class UnnamedConstantString(value: String)
    extends UnnamedColumnSpec[String] {
  def withName(s: String): NamedColumnSpec[String] =
    NamedConstantString(value, s)
}
private[ra3] final case class UnnamedConstantF64(value: Double)
    extends UnnamedColumnSpec[Double] {
  def withName(s: String): NamedColumnSpec[Double] = NamedConstantF64(value, s)
}
private[ra3] object StarColumnSpec extends ColumnSpec[Any]
