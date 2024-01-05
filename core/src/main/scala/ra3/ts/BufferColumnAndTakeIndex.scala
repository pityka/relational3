package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

case class BufferColumnAndTakeIndex(
    input: Column[_],
    idx: Option[SegmentInt],
    outputPath: LogicalPath
)
object BufferColumnAndTakeIndex {
  def queue[D<:DataType](input: Column[D], idx: Option[SegmentInt], outputPath: LogicalPath)(
      implicit tsc: TaskSystemComponents
  ): IO[D#SegmentType] =
    task(BufferColumnAndTakeIndex(input, idx, outputPath))(
      ResourceRequest(cpu = (1, 1), memory = 1, scratch = 0, gpu = 0)
    ).map(s => input.dataType.cast(s))

  implicit val codec: JsonValueCodec[BufferColumnAndTakeIndex] =
    JsonCodecMaker.make

  private def doit(input: Column[_], idx: Option[SegmentInt], outputPath: LogicalPath)(
      implicit tsc: TaskSystemComponents
  ) = {
    val bufferedColumn = IO
      .parSequenceN(32)(
        input.segments.map(_.buffer)
      )
      .map(_.reduce(_ ++ _))
    val bufferedIdx =
      idx.map(_.buffer.map(Some(_))).getOrElse(IO.pure(None))

    IO.both(bufferedColumn, bufferedIdx)
      .flatMap { case (part, idx) =>
        idx
          .map(t => part.take(t))
          .getOrElse(part)
          .toSegment(outputPath)
      }
  }

  val task =
    Task[BufferColumnAndTakeIndex, Segment[_]]("BufferColumnAndTakeIndex", 1) {
      case input =>
        implicit ce => doit(input.input, input.idx, input.outputPath)

    }
}
