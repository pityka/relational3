package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

case class ImportCsv(
    file: SharedFile,
    name: String,
    columns: Seq[(Int, ColumnTag, Option[ImportCsv.InstantFormat])],
    recordSeparator: String,
    fieldSeparator: Char,
    header: Boolean,
    maxLines: Long,
    maxSegmentLength: Int,
    compression: Option[ImportCsv.CompressionFormat],
    bufferSize: Int
)
object ImportCsv {
  sealed trait CompressionFormat
  case object Gzip extends CompressionFormat

  sealed trait InstantFormat
  case object ISO extends InstantFormat
  case class LocalDateTimeAtUTC(s:String) extends InstantFormat
  def queue(
      file: SharedFile,
      name: String,
      columns: Seq[(Int, ColumnTag, Option[ImportCsv.InstantFormat])],
      recordSeparator: String,
      fieldSeparator: Char,
      header: Boolean,
      maxLines: Long,
      maxSegmentLength: Int,
      compression: Option[CompressionFormat],
      bufferSize: Int
  )(implicit
      tsc: TaskSystemComponents
  ): IO[Table] = {

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
        bufferSize
      )
    )(
      ResourceRequest(
        cpu = (1, 1),
        memory = ra3.Utils.guessMemoryUsageInMB(maxSegmentLength),
        scratch = 0,
        gpu = 0
      )
    )
  }

  private def doit(
      file: SharedFile,
      name: String,
      columns: Seq[(Int, ColumnTag, Option[InstantFormat])],
      recordSeparator: String,
      fieldSeparator: Char,
      header: Boolean,
      maxLines: Long,
      maxSegmentLength: Int,
      compression: Option[CompressionFormat],
      bufferSize : Int
  )(implicit tsc: TaskSystemComponents): IO[Table] = {

    val rawStream = file.stream
    
    fs2.io.toInputStreamResource(rawStream).use { is =>
      IO {
        val is2 = compression match {
          case None => is 
          case Some(Gzip) =>  new java.util.zip.GZIPInputStream(is)
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
                  case Some(ISO) => Some(ra3.InstantParser.ISO)
                  case Some(LocalDateTimeAtUTC(s)) => Some(ra3.InstantParser.LocalDateTimeAtUTC(s))
                  case None      => None
                }
              )
            ),
            channel = channel,
            header = header,
            maxLines = maxLines,
            maxSegmentLength = maxSegmentLength,
            fieldSeparator = fieldSeparator,
            recordSeparator = recordSeparator,
            bufferSize = bufferSize
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
    }

  }

  implicit val codec: JsonValueCodec[ImportCsv] = JsonCodecMaker.make
  implicit val codecOut: JsonValueCodec[Table] = JsonCodecMaker.make
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
        input.bufferSize
      )

  }
}
