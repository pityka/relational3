package ra3

/** Enum for predefined compression formats */
sealed trait CompressionFormat
object CompressionFormat {
  case object Gzip extends CompressionFormat
}

/** Enum for predefined formats parsing string to Instant */
sealed trait InstantFormat
object InstantFormat {
  case object ISO extends InstantFormat

  /** InstantFormat with a format string for LocalDateTime
    *
    * The given format string will be passed to java.time.LocalDateTime.parse
    */
  case class LocalDateTimeAtUTC(s: String) extends InstantFormat
}

/** Enum for predefined character decoders */
sealed trait CharacterDecoder {
  def silent: Boolean
}

object CharacterDecoder {
  case class UTF8(silent: Boolean) extends CharacterDecoder
  case class ASCII(silent: Boolean) extends CharacterDecoder
  case class ISO88591(silent: Boolean) extends CharacterDecoder
  case class UTF16(silent: Boolean) extends CharacterDecoder

}

/** Enum for predefined column types */
sealed trait CSVColumnDefinition
object CSVColumnDefinition {

  /** Int (int32) column type with its 0-based column index in the CSV file */
  case class I32Column(columnIndex: Int) extends CSVColumnDefinition

  /** Long (int64) column type with its 0-based column index in the CSV file */
  case class I64Column(columnIndex: Int) extends CSVColumnDefinition

  /** Double (64 bit floating point) column type with its 0-based column index
    * in the CSV file
    */
  case class F64Column(columnIndex: Int) extends CSVColumnDefinition

  /** Variable length string column type with its 0-based column index in the
    * CSV file
    *
    * @param missingValue
    *   each exact match with this string is treated as missing value
    */
  case class StrColumn(columnIndex: Int, missingValue: Option[String] = None)
      extends CSVColumnDefinition

  /** Instant column type with its 0-based column index in the CSV file
    *
    * Values are stored as milliseconds since epoch in 64-bit signed integers
    * (long)
    *
    * @param parser
    *   String to java.time.Instant format
    */
  case class InstantColumn(
      columnIndex: Int,
      parser: Option[InstantFormat] = None
  ) extends CSVColumnDefinition
}
