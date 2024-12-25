package ra3.lang

import ra3.*
import java.time.Instant

private[ra3] sealed trait ColumnSpec[N, +T] {
  def value: T
  def name: String
}

private[ra3] sealed trait NamedColumnSpecWithColumnChunkValue[N, +T]
    extends ColumnSpec[N, T]

private[ra3] final case class NamedColumnChunkI32[N](
    value: I32Var,
    name: String
) extends NamedColumnSpecWithColumnChunkValue[N, I32Var]
private[ra3] final case class NamedColumnChunkI64[N](
    value: I64Var,
    name: String
) extends NamedColumnSpecWithColumnChunkValue[N, I64Var]

private[ra3] final case class NamedColumnChunkF64[N](
    value: F64Var,
    name: String
) extends NamedColumnSpecWithColumnChunkValue[N, F64Var]

private[ra3] final case class NamedColumnChunkStr[N](
    value: StrVar,
    name: String
) extends NamedColumnSpecWithColumnChunkValue[N, StrVar]

private[ra3] final case class NamedColumnChunkInst[N](
    value: InstVar,
    name: String
) extends NamedColumnSpecWithColumnChunkValue[N, InstVar]

// Constants

private[ra3] final case class NamedConstantI32[N](value: Int, name: String)
    extends ColumnSpec[N, Int]
private[ra3] final case class NamedConstantI64[N](value: Long, name: String)
    extends ColumnSpec[N, Long]
private[ra3] final case class NamedConstantString[N](
    value: String,
    name: String
) extends ColumnSpec[N, String]
private[ra3] final case class NamedConstantF64[N](value: Double, name: String)
    extends ColumnSpec[N, Double]
private[ra3] final case class NamedConstantInstant[N](
    value: Instant,
    name: String
) extends ColumnSpec[N, Instant]

object NamedColumnSpecWithColumnChunkValueExtractor {
  def unapply(
      t: ColumnSpec[?, ?]
  ): Option[(Either[TaggedBuffer, TaggedSegments], String)] = {
    t match {
      case NamedColumnChunkI32(value, name) =>
        Some(
          (
            value.v
              .map(ColumnTag.I32.makeTaggedSegments)
              .left
              .map(ColumnTag.I32.makeTaggedBuffer),
            name
          )
        )
      case NamedColumnChunkI64(value, name) =>
        Some(
          (
            value.v
              .map(ColumnTag.I64.makeTaggedSegments)
              .left
              .map(ColumnTag.I64.makeTaggedBuffer),
            name
          )
        )
      case NamedColumnChunkF64(value, name) =>
        Some(
          (
            value.v
              .map(ColumnTag.F64.makeTaggedSegments)
              .left
              .map(ColumnTag.F64.makeTaggedBuffer),
            name
          )
        )
      case NamedColumnChunkStr(value, name) =>
        Some(
          (
            value.v
              .map(ColumnTag.StringTag.makeTaggedSegments)
              .left
              .map(ColumnTag.StringTag.makeTaggedBuffer),
            name
          )
        )
      case NamedColumnChunkInst(value, name) =>
        Some(
          (
            value.v
              .map(ColumnTag.Instant.makeTaggedSegments)
              .left
              .map(ColumnTag.Instant.makeTaggedBuffer),
            name
          )
        )
      // case ra3.lang.NamedAnyColumnSpec(_, name) => ???
      case ra3.lang.NamedConstantI32(_, _)     => None
      case ra3.lang.NamedConstantI64(_, _)     => None
      case ra3.lang.NamedConstantString(_, _)  => None
      case ra3.lang.NamedConstantF64(_, _)     => None
      case ra3.lang.NamedConstantInstant(_, _) => None
    }

  }
}
