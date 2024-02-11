import tasks._

package object ra3 extends ColumnTags {
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
    compression
  )
}
