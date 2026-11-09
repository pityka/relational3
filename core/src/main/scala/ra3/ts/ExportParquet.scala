package ra3.ts

import ra3.*
import ra3.parquet.{Parquet, Column}
import tasks.*
import tasks.jsonitersupport.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import cats.effect.IO

private[ra3] case class ExportParquet(
    segments: Seq[TaggedSegment],
    colNames: Seq[String],
    outputName: String,
    outputSegmentIndex: Int,
    snappy: Boolean
)
private[ra3] object ExportParquet {

  def queue(
      segments: Seq[TaggedSegment],
      colNames: Seq[String],
      outputName: String,
      outputSegmentIndex: Int,
      snappy: Boolean
  )(implicit
      tsc: ExtendedTaskSystemComponents
  ): IO[SharedFile] = {
    IO {
      scribe.debug(
        s"Queueing ExportParquet on ${segments.size} segments, total rows: ${segments.map(_.segment.numElems.toLong).sum}"
      )
    } *>
      task(
        ExportParquet(segments, colNames, outputName, outputSegmentIndex, snappy)
      )(
        ResourceRequest(
          cpu = (1, 1),
          memory =
            segments.map(_.segment).map(ra3.Utils.guessMemoryUsageInMB).sum * 10,
          scratch = 0,
          gpu = 0
        )
      )
  }

  private def columnFromBuffer(name: String, buffer: Buffer): Column = {
    val n = buffer.length
    val defLevels = Array.tabulate(n)(i => if (buffer.isMissing(i)) 0 else 1)
    val opt = defLevels.exists(_ == 0)
    val emptyLevels = Array.empty[Int]
    buffer match {
      case b: BufferInt =>
        val present =
          (0 until n).filterNot(b.isMissing).map(i => b.values(i)).toArray
        val mm = b.nonMissingMinMax
        Column(name, Parquet.TYPE_INT32, None, opt, n, present.length,
          if (opt) defLevels else emptyLevels, Parquet.plainI32(present),
          mm.map(x => Parquet.plainI32(Array(x._1))),
          mm.map(x => Parquet.plainI32(Array(x._2))))
      case b: BufferLong =>
        val present =
          (0 until n).filterNot(b.isMissing).map(i => b.values(i)).toArray
        val mm = b.nonMissingMinMax
        Column(name, Parquet.TYPE_INT64, None, opt, n, present.length,
          if (opt) defLevels else emptyLevels, Parquet.plainI64(present),
          mm.map(x => Parquet.plainI64(Array(x._1))),
          mm.map(x => Parquet.plainI64(Array(x._2))))
      case b: BufferInstant =>
        val present =
          (0 until n).filterNot(b.isMissing).map(i => b.values(i)).toArray
        val mm = b.nonMissingMinMax
        Column(name, Parquet.TYPE_INT64, Some(Parquet.CONV_TIMESTAMP_MILLIS),
          opt, n, present.length,
          if (opt) defLevels else emptyLevels, Parquet.plainI64(present),
          mm.map(x => Parquet.plainI64(Array(x._1))),
          mm.map(x => Parquet.plainI64(Array(x._2))))
      case b: BufferDouble =>
        val present =
          (0 until n).filterNot(b.isMissing).map(i => b.values(i)).toArray
        val mm = b.nonMissingMinMax
        Column(name, Parquet.TYPE_DOUBLE, None, opt, n, present.length,
          if (opt) defLevels else emptyLevels, Parquet.plainF64(present),
          mm.map(x => Parquet.plainF64(Array(x._1))),
          mm.map(x => Parquet.plainF64(Array(x._2))))
      case b: BufferString =>
        val present = (0 until n)
          .filterNot(b.isMissing)
          .map(i => b.elementAsCharSequence(i).toString.getBytes("UTF-8"))
          .toArray
        val mm = b.nonMissingMinMax
        Column(name, Parquet.TYPE_BYTE_ARRAY, Some(Parquet.CONV_UTF8),
          opt, n, present.length,
          if (opt) defLevels else emptyLevels, Parquet.plainBytes(present),
          mm.map(x => x._1.toString.getBytes("UTF-8")),
          mm.map(x => x._2.toString.getBytes("UTF-8")))
    }
  }

  private def doit(
      segments: Seq[TaggedSegment],
      colNames: Seq[String],
      outputName: String,
      outputSegmentIndex: Int,
      snappy: Boolean
  )(implicit tsc: ComputationEnvironment): IO[SharedFile] = {
    assert(segments.nonEmpty)
    IO.parSequenceN(32)(segments.map(v => v.tag.buffer(v.segment)))
      .map(_.toVector)
      .flatMap { buffers =>
        IO {
          val numRows = segments.head.segment.numElems
          assert(segments.forall(_.segment.numElems == numRows))
          val cols = buffers.zip(colNames).map { case (b, nm) =>
            columnFromBuffer(nm, b)
          }
          val bytes = Parquet.writeFile(
            cols,
            numRows,
            if (snappy) Parquet.CODEC_SNAPPY else Parquet.CODEC_UNCOMPRESSED
          )
          val stream = fs2.Stream.chunk(fs2.Chunk.array(bytes))
          SharedFile.scoped(
            stream,
            s"${outputName}/parquet/$outputSegmentIndex.parquet"
          )
        }.flatten
      }
      .logElapsed
  }

  // $COVERAGE-OFF$
  implicit val codec: JsonValueCodec[ExportParquet] = JsonCodecMaker.make
  // $COVERAGE-ON$
  val task = Task[ExportParquet, SharedFile]("ExportParquet", 1) { case input =>
    implicit ce =>
      doit(
        input.segments,
        input.colNames,
        input.outputName,
        input.outputSegmentIndex,
        input.snappy
      )
  }
}
