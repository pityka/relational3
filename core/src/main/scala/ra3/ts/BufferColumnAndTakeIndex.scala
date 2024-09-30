package ra3.ts

import ra3.*
import tasks.*
import tasks.jsonitersupport.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import cats.effect.IO

private[ra3] case class BufferColumnAndTakeIndex(
    input: TaggedColumn,
    idx: Option[SegmentInt],
    outputPath: LogicalPath
)
private[ra3] object BufferColumnAndTakeIndex {
  def queue(
      input: TaggedColumn,
      idx: Option[SegmentInt],
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ): IO[input.tag.SegmentType] =
    task(BufferColumnAndTakeIndex(input, idx, outputPath))(
      ResourceRequest(
        cpu = (1, 1),
        memory = ra3.Utils.guessMemoryUsageInMB(input),
        scratch = 0,
        gpu = 0
      )
    ).map((_: Segment).asInstanceOf[input.tag.SegmentType])

  implicit val codec: JsonValueCodec[BufferColumnAndTakeIndex] =
    JsonCodecMaker.make

  implicit val codecOut: JsonValueCodec[Segment] = JsonCodecMaker.make

  private def doit(
      input: TaggedColumn,
      idx: Option[SegmentInt],
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ) = {

    if (idx.isDefined && idx.get.numElems == 0) IO.pure(input.tag.emptySegment)
    else {
      val bufferedColumn = IO
        .parSequenceN(32)(
          input.tag.segments(input.column).map(s => input.tag.buffer(s))
        )
        .map(b => input.tag.cat(b*))

      val bufferedIdx =
        idx.map(_.buffer.map(Some(_))).getOrElse(IO.pure(None))
      IO.both(bufferedColumn, bufferedIdx)
        .flatMap { case (part, idx) =>
          input.tag.toSegment(
            idx
              .map(t => input.tag.take(part, t))
              .getOrElse(part),
            outputPath
          )
        }
    }
  }

  val task =
    Task[BufferColumnAndTakeIndex, Segment]("BufferColumnAndTakeIndex", 1) {
      case input =>
        implicit ce => doit(input.input, input.idx, input.outputPath)

    }
}
