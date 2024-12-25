package ra3.ts

import ra3.*
import tasks.*
import tasks.jsonitersupport.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import cats.effect.IO
import java.nio.charset.StandardCharsets
import java.nio.charset.CodingErrorAction

private[ra3] case class ImportCsv(
    file: SharedFile,
    name: String,
    columns: Seq[
      (Int, ColumnTag, Option[InstantFormat], Option[String])
    ],
    recordSeparator: String,
    fieldSeparator: Char,
    header: Boolean,
    maxLines: Long,
    maxSegmentLength: Int,
    compression: Option[CompressionFormat],
    bufferSize: Int,
    characterDecoder: CharacterDecoder
)
private[ra3] object ImportCsv {

  def queue(
      file: SharedFile,
      name: String,
      columns: Seq[
        (Int, ColumnTag, Option[InstantFormat], Option[String])
      ],
      recordSeparator: String,
      fieldSeparator: Char,
      header: Boolean,
      maxLines: Long,
      maxSegmentLength: Int,
      compression: Option[CompressionFormat],
      bufferSize: Int,
      characterDecoder: CharacterDecoder
  )(implicit
      tsc: TaskSystemComponents
  ): IO[Table] = {
    IO {
      scribe.debug(
        s"Queueing ImportCSV of ${file} ${columns.size} columns"
      )
    } *>
      task(
        ImportCsv(
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
      )(
        ResourceRequest(
          cpu = (1, 1),
          memory =
            ra3.Utils.guessMemoryUsageInMB(maxSegmentLength) * columns.size,
          scratch = 0,
          gpu = 0
        )
      )
  }

  private def doit(
      file: SharedFile,
      name: String,
      columns: Seq[
        (Int, ColumnTag, Option[InstantFormat], Option[CharSequence])
      ],
      recordSeparator: String,
      fieldSeparator: Char,
      header: Boolean,
      maxLines: Long,
      maxSegmentLength: Int,
      compression: Option[CompressionFormat],
      bufferSize: Int,
      characterDecoder: CharacterDecoder
  )(implicit tsc: TaskSystemComponents): IO[Table] = {
    scribe.info("Start import csv")
    val rawStream = file.stream

    val charset = {
      val c1 = characterDecoder match {
        case CharacterDecoder.ASCII(_) =>
          StandardCharsets.US_ASCII
            .newDecoder()
        case (CharacterDecoder.UTF8(_)) =>
          StandardCharsets.UTF_8
            .newDecoder()
        case (CharacterDecoder.ISO88591(_)) =>
          StandardCharsets.ISO_8859_1
            .newDecoder()
        case (CharacterDecoder.UTF16(_)) =>
          StandardCharsets.UTF_16
            .newDecoder()
      }

      if (characterDecoder.silent) {
        c1.onMalformedInput(CodingErrorAction.REPLACE)
          .onUnmappableCharacter(CodingErrorAction.REPLACE)
      } else c1

    }

    fs2.io.toInputStreamResource(rawStream).use { is =>
      IO.blocking {
        val is2 = compression match {
          case None => is
          case Some(CompressionFormat.Gzip) =>
            Utils.gzip(is)
        }
        val channel = java.nio.channels.Channels.newChannel(is2)
        val result = ra3.csv
          .readHeterogeneousFromCSVChannel(
            name = name,
            columnTypes = columns.map(v =>
              (
                v._1,
                v._2,
                v._3 match {
                  case Some(InstantFormat.ISO) => Some(ra3.InstantParser.ISO)
                  case Some(InstantFormat.LocalDateTimeAtUTC(s)) =>
                    Some(ra3.InstantParser.LocalDateTimeAtUTC(s))
                  case None => None
                },
                v._4
              )
            ),
            channel = channel,
            header = header,
            maxLines = maxLines,
            maxSegmentLength = maxSegmentLength,
            fieldSeparator = fieldSeparator,
            recordSeparator = recordSeparator,
            bufferSize = bufferSize,
            charset = charset
          )
        result
      }.flatMap { result =>
        result match {
          case Left(value) =>
            scribe.error(s"Error from csv parser: $value")
            IO.raiseError(
              new RuntimeException(s"Error from csv parser: $value")
            )
          case Right(value) =>
            IO.pure(value)
        }
      }
    }.logElapsed

  }
  // $COVERAGE-OFF$
  implicit val codec: JsonValueCodec[ImportCsv] = JsonCodecMaker.make
  implicit val codecOut: JsonValueCodec[Table] = JsonCodecMaker.make
  // $COVERAGE-ON$
  val task = Task[ImportCsv, Table]("ImportCsv", 1) { case input =>
    implicit ce =>
      doit(
        input.file,
        input.name,
        input.columns,
        input.recordSeparator,
        input.fieldSeparator,
        input.header,
        input.maxLines,
        input.maxSegmentLength,
        input.compression,
        input.bufferSize,
        input.characterDecoder
      )

  }
}
