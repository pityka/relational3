package ra3.parquet

import ra3.*
import Parquet.*

private[ra3] object ScanSupport {

  def tagFor(ptype: Int, convertedType: Int): ColumnTag = ptype match {
    case TYPE_INT32 => ColumnTag.I32
    case TYPE_INT64 if convertedType == CONV_TIMESTAMP_MILLIS =>
      ColumnTag.Instant
    case TYPE_INT64            => ColumnTag.I64
    case TYPE_DOUBLE | TYPE_FLOAT => ColumnTag.F64
    case TYPE_BYTE_ARRAY       => ColumnTag.StringTag
    case other =>
      throw new RuntimeException(s"unsupported parquet physical type $other")
  }

  private def requireMinMax(cm: PColMeta): (Array[Byte], Array[Byte]) =
    (cm.minBytes, cm.maxBytes) match {
      case (Some(mn), Some(mx)) => (mn, mx)
      case _ =>
        throw new RuntimeException(
          s"scanParquet requires column statistics (min/max); column '${cm.name}' has none. Use importParquet for this file."
        )
    }

  def buildSegment(
      file: tasks.SharedFile,
      cm: PColMeta,
      numRows: Int,
      maxRep: Int,
      maxDef: Int
  ): Segment = {
    val ref = ParquetSegmentRef(
      file,
      cm.ptype,
      cm.codec,
      cm.numValues,
      cm.dataPageOffset,
      cm.dictPageOffset,
      cm.totalCompressed,
      cm.optional,
      maxRep,
      maxDef
    )
    val hasMissing = cm.nullCount != 0L
    cm.ptype match {
      case TYPE_INT32 =>
        val (mn, mx) = requireMinMax(cm)
        SegmentInt(
          None,
          numRows,
          StatisticInt(
            hasMissing,
            Some(
              (
                statValue(cm.ptype, mn).asInstanceOf[Int],
                statValue(cm.ptype, mx).asInstanceOf[Int]
              )
            ),
            None
          ),
          Some(ref)
        )
      case TYPE_INT64 if cm.convertedType == CONV_TIMESTAMP_MILLIS =>
        val (mn, mx) = requireMinMax(cm)
        SegmentInstant(
          None,
          numRows,
          StatisticLong(
            hasMissing,
            Some(
              (
                statValue(cm.ptype, mn).asInstanceOf[Long],
                statValue(cm.ptype, mx).asInstanceOf[Long]
              )
            ),
            None,
            None
          ),
          Some(ref)
        )
      case TYPE_INT64 =>
        val (mn, mx) = requireMinMax(cm)
        SegmentLong(
          None,
          numRows,
          StatisticLong(
            hasMissing,
            Some(
              (
                statValue(cm.ptype, mn).asInstanceOf[Long],
                statValue(cm.ptype, mx).asInstanceOf[Long]
              )
            ),
            None,
            None
          ),
          Some(ref)
        )
      case TYPE_DOUBLE | TYPE_FLOAT =>
        val (mn, mx) = requireMinMax(cm)
        SegmentDouble(
          None,
          numRows,
          StatisticDouble(
            hasMissing,
            Some(
              (
                statValue(cm.ptype, mn).asInstanceOf[Double],
                statValue(cm.ptype, mx).asInstanceOf[Double]
              )
            ),
            None
          ),
          Some(ref)
        )
      case TYPE_BYTE_ARRAY =>
        val (mn, mx) = requireMinMax(cm)
        SegmentString(
          None,
          numRows,
          cm.totalCompressed,
          StatisticCharSequence(
            hasMissing,
            Some(
              (
                (new String(mn, "UTF-8"): CharSequence),
                (new String(mx, "UTF-8"): CharSequence)
              )
            ),
            None,
            None
          ),
          Some(ref)
        )
      case other =>
        throw new RuntimeException(s"unsupported parquet physical type $other")
    }
  }
}
