package ra3.ts

import ra3.*
import tasks.*
import tasks.jsonitersupport.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import cats.effect.IO
import de.lhns.fs2.compress.GzipCompressor
import java.nio.CharBuffer
import java.nio.charset.StandardCharsets

private[ra3] case class ExportCsv(
    segments: Seq[TaggedSegment],
    columnSeparator: Char,
    quoteChar: Char,
    recordSeparator: String,
    outputName: String,
    outputSegmentIndex: Int,
    compression: Option[ExportCsv.CompressionFormat]
)
private[ra3] object ExportCsv {
  sealed trait CompressionFormat
  case object Gzip extends CompressionFormat

  def queue(
      segments: Seq[TaggedSegment],
      columnSeparator: Char,
      quoteChar: Char,
      recordSeparator: String,
      outputName: String,
      outputSegmentIndex: Int,
      compression: Option[ExportCsv.CompressionFormat]
  )(implicit
      tsc: TaskSystemComponents
  ): IO[SharedFile] = {
    IO {
      scribe.debug(
        s"Queueing ExportCSV on ${segments.size} segments, total rows: ${segments.map(_.segment.numElems.toLong).sum}"
      )
    } *>
      task(
        ExportCsv(
          segments,
          columnSeparator,
          quoteChar,
          recordSeparator,
          outputName,
          outputSegmentIndex,
          compression
        )
      )(
        ResourceRequest(
          cpu = (1, 1),
          memory = segments
            .map(_.segment)
            .map(ra3.Utils.guessMemoryUsageInMB)
            .sum * 10,
          scratch = 0,
          gpu = 0
        )
      )
  }

  private def doit(
      segments: Seq[TaggedSegment],
      columnSeparator: Char,
      quoteChar: Char,
      recordSeparator: String,
      outputName: String,
      outputSegmentIndex: Int,
      compression: Option[ExportCsv.CompressionFormat]
  )(implicit tsc: TaskSystemComponents): IO[SharedFile] = {
    scribe.debug("ExportCSV start.")
    def contains(cs: CharSequence, c: Char) = {
      var i = 0
      val n = cs.length
      var b = false
      while (i < n && !b) {
        b = cs.charAt(i) == c
        i += 1
      }
      b
    }

    def quote(s: CharSequence): CharSequence =
      if (contains(s, columnSeparator)) s"$quoteChar$s$quoteChar" else s

    val columnSeparatorStr = columnSeparator.toString

    assert(segments.nonEmpty)
    IO.parSequenceN(32)(segments.map(v => v.tag.buffer(v.segment)))
      .map(_.toVector)
      .flatMap { buffers =>
        IO {
          val numRows = segments.head.segment.numElems
          val numCols = buffers.length
          assert(segments.forall(_.segment.numElems == numRows))
          val sb = new StringBuilder
          0 until numRows foreach { rowIdx =>
            0 until numCols foreach { colIdx =>
              sb.append(quote(buffers(colIdx).elementAsCharSequence(rowIdx)))
              if (colIdx == numCols - 1) {
                sb.append(recordSeparator)
              } else sb.append(columnSeparatorStr)
            }
          }

          val bytes = StandardCharsets.UTF_8.encode(CharBuffer.wrap(sb))

          val uncompressedStream = fs2.Stream.chunk(fs2.Chunk.byteBuffer(bytes))

          val stream = compression match {
            case None => uncompressedStream
            case Some(Gzip) =>
              implicit val c: GzipCompressor[IO] =
                GzipCompressor.make()
              uncompressedStream.through(GzipCompressor[IO].compress)
          }

          val suf = compression match {
            case None       => ""
            case Some(Gzip) => ".gz"
          }

          SharedFile.apply(
            stream,
            s"${outputName}/csv/$outputSegmentIndex.csv$suf"
          )
        }.flatten
      }

  }
  // $COVERAGE-OFF$
  implicit val codec: JsonValueCodec[ExportCsv] = JsonCodecMaker.make
  // $COVERAGE-ON$
  val task = Task[ExportCsv, SharedFile]("ExportCsv", 1) { case input =>
    implicit ce =>
      doit(
        input.segments,
        input.columnSeparator,
        input.quoteChar,
        input.recordSeparator,
        input.outputName,
        input.outputSegmentIndex,
        input.compression
      )

  }
}
