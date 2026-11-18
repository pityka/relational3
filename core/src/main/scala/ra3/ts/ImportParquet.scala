package ra3.ts

import ra3.*
import _root_.parquet.{Parquet, ParquetReader, ReadColumn}
import tasks.*
import tasks.jsonitersupport.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import cats.effect.IO

private[ra3] case class ImportParquet(
    file: SharedFile,
    name: String,
    maxSegmentLength: Int
)
private[ra3] object ImportParquet {

  def queue(
      file: SharedFile,
      name: String,
      maxSegmentLength: Int
  )(implicit
      tsc: ExtendedTaskSystemComponents
  ): IO[Table] = {
    IO {
      scribe.debug(s"Queueing ImportParquet of ${file.name}")
    } *> task(
      ImportParquet(file, name, maxSegmentLength)
    )(
      ResourceRequest(
        cpu = (1, 1),
        memory = (file.byteSize / 1000000L).toInt * 8 + 500,
        scratch = 0,
        gpu = 0
      )
    )
  }

  private def tagFor(rc: ReadColumn): ColumnTag = rc.ptype match {
    case ParquetReader.TYPE_INT32 => ColumnTag.I32
    case ParquetReader.TYPE_INT64
        if rc.convertedType == ParquetReader.CONV_TIMESTAMP_MILLIS =>
      ColumnTag.Instant
    case ParquetReader.TYPE_INT64 => ColumnTag.I64
    case ParquetReader.TYPE_DOUBLE | ParquetReader.TYPE_FLOAT => ColumnTag.F64
    case ParquetReader.TYPE_BYTE_ARRAY => ColumnTag.StringTag
    case other =>
      throw new RuntimeException(s"unsupported parquet physical type $other")
  }

  private def sliceToSegment(
      rc: ReadColumn,
      path: LogicalPath,
      from: Int,
      to: Int
  )(implicit tsc: ComputationEnvironment): IO[Segment] = {
    val len = to - from
    rc match {
      case c: ReadColumn.Ints =>
        val arr = Array.tabulate(len)(k =>
          if (c.nulls(from + k)) Int.MinValue else c.values(from + k)
        )
        ColumnTag.I32.toSegment(BufferInt(arr), path)
      case c: ReadColumn.Longs
          if c.convertedType == ParquetReader.CONV_TIMESTAMP_MILLIS =>
        val arr = Array.tabulate(len)(k =>
          if (c.nulls(from + k)) Long.MinValue else c.values(from + k)
        )
        ColumnTag.Instant.toSegment(BufferInstant(arr), path)
      case c: ReadColumn.Longs =>
        val arr = Array.tabulate(len)(k =>
          if (c.nulls(from + k)) Long.MinValue else c.values(from + k)
        )
        ColumnTag.I64.toSegment(BufferLong(arr), path)
      case c: ReadColumn.Doubles =>
        val arr = Array.tabulate(len)(k =>
          if (c.nulls(from + k)) Double.NaN else c.values(from + k)
        )
        ColumnTag.F64.toSegment(BufferDouble(arr), path)
      case c: ReadColumn.Strings =>
        val arr = Array.tabulate[CharSequence](len)(k =>
          if (c.nulls(from + k)) BufferString.missing
          else (c.values(from + k): CharSequence)
        )
        ColumnTag.StringTag.toSegment(BufferString(arr), path)
    }
  }

  private def buildColumn(
      rc: ReadColumn,
      name: String,
      colIdx: Int,
      maxSegmentLength: Int,
      numRows: Int
  )(implicit tsc: ComputationEnvironment): IO[TaggedColumn] = {
    val tag = tagFor(rc)
    val numSeg =
      if (numRows == 0) 1 else (numRows + maxSegmentLength - 1) / maxSegmentLength
    val segIOs = (0 until numSeg).toList.map { segIdx =>
      val from = segIdx * maxSegmentLength
      val to = math.min(numRows, from + maxSegmentLength)
      sliceToSegment(rc, LogicalPath(name, None, segIdx, colIdx), from, to)
    }
    IO.parSequenceN(32)(segIOs).map { segs =>
      tag.makeTaggedColumn(
        tag.makeColumn(segs.map(_.asInstanceOf[tag.SegmentType]).toVector)
      )
    }
  }

  private def doit(
      file: SharedFile,
      name: String,
      maxSegmentLength: Int
  )(implicit tsc: ComputationEnvironment): IO[Table] = {
    file.bytes.flatMap { byteVector =>
      buildTable(
        Parquet.assembleAuto(byteVector.toArray),
        name,
        maxSegmentLength
      )
    }.logElapsed
  }

  private[ra3] def buildTable(
      cols: Vector[ReadColumn],
      name: String,
      maxSegmentLength: Int
  )(implicit tsc: ComputationEnvironment): IO[Table] = {
    val numRows = cols.headOption.map(_.length).getOrElse(0)
    IO.parSequenceN(math.min(32, math.max(1, cols.size)))(
      cols.zipWithIndex.toList.map { case (rc, colIdx) =>
        buildColumn(rc, name, colIdx, maxSegmentLength, numRows)
      }
    ).map { taggedCols =>
      Table(taggedCols.toVector, cols.map(_.name).toVector, name, None)
    }
  }

  // $COVERAGE-OFF$
  implicit val codec: JsonValueCodec[ImportParquet] = JsonCodecMaker.make
  implicit val codecOut: JsonValueCodec[Table] = JsonCodecMaker.make
  // $COVERAGE-ON$
  val task = Task[ImportParquet, Table]("ImportParquet", 1) { case input =>
    implicit ce =>
      doit(input.file, input.name, input.maxSegmentLength)
  }
}
