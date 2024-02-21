import tasks._

package object ra3 extends ColumnTags {
  val MissingString = BufferString.MissingValue.toString

  type ColumnVariable[T] = ra3.lang.DelayedIdent[T]

  type TableVariable = ra3.tablelang.TableExpr.Ident

  sealed trait CSVColumnDefinition
  case class I32Column(columnIndex: Int) extends CSVColumnDefinition
  case class I64Column(columnIndex: Int) extends CSVColumnDefinition
  case class F64Column(columnIndex: Int) extends CSVColumnDefinition
  case class StrColumn(columnIndex: Int, missingValue: Option[String] = None)
      extends CSVColumnDefinition
  case class InstantColumn(
      columnIndex: Int,
      parser: Option[ra3.ts.ImportCsv.InstantFormat] = None
  ) extends CSVColumnDefinition

  def importCsv(
      file: SharedFile,
      name: String,
      columns: Seq[CSVColumnDefinition],
      maxSegmentLength: Int,
      compression: Option[ra3.ts.ImportCsv.CompressionFormat] = None,
      recordSeparator: String = "\r\n",
      fieldSeparator: Char = ',',
      header: Boolean = false,
      maxLines: Long = Long.MaxValue,
      bufferSize: Int = 8292,
      characterDecoder: ra3.ts.ImportCsv.CharacterDecoder =
        ra3.ts.ImportCsv.ASCII(silent = true)
  )(implicit
      tsc: TaskSystemComponents
  ) = ra3.ts.ImportCsv.queue(
    file,
    name,
    columns.map {
      case I32Column(columnIndex) => (columnIndex, ColumnTag.I32, None, None)
      case I64Column(columnIndex) => (columnIndex, ColumnTag.I64, None, None)
      case F64Column(columnIndex) => (columnIndex, ColumnTag.F64, None, None)
      case StrColumn(columnIndex, missingValue) =>
        (columnIndex, ColumnTag.StringTag, None, missingValue)
      case InstantColumn(columnIndex, parser) =>
        (columnIndex, ColumnTag.Instant, parser, None)
    },
    recordSeparator,
    fieldSeparator,
    header,
    maxLines,
    maxSegmentLength,
    compression,
    bufferSize,
    characterDecoder
  )
}
