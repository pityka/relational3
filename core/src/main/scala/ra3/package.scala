import tasks._

package object ra3 extends ColumnTags {
  val MissingString = BufferString.MissingValue.toString
  def importCsv(
      file: SharedFile,
      name: String,
      columns: Seq[(Int, ColumnTag, Option[ra3.ts.ImportCsv.InstantFormat])],
      maxSegmentLength: Int,
      compression: Option[ra3.ts.ImportCsv.CompressionFormat] = None,
      recordSeparator: String = "\r\n",
      fieldSeparator: Char = ',',
      header: Boolean = false,
      maxLines: Long = Long.MaxValue,
      bufferSize: Int = 8292,
      characterDecoder: ra3.ts.ImportCsv.CharacterDecoder = ra3.ts.ImportCsv.ASCII(silent=true)
  )(implicit
      tsc: TaskSystemComponents
  ) = ra3.ts.ImportCsv.queue(
    file,
    name,
    columns,
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
