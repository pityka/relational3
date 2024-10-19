package ra3.ts

import ra3.*
import tasks.*
import tasks.jsonitersupport.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import cats.effect.IO

private[ra3] case class TakeIndex(
    input: TaggedSegment,
    idx: SegmentInt,
    outputPath: LogicalPath
)
private[ra3] object TakeIndex {
  def queue(tag: ColumnTag)(
      input: tag.SegmentType,
      idx: SegmentInt,
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ): IO[tag.SegmentType] =
    IO {
      scribe.debug(
        s"Queueing TakeIndex on $tag type ${input.numElems} and ${idx.numElems} sizes"
      )
    } *> task(TakeIndex(tag.makeTaggedSegment(input), idx, outputPath))(
      ResourceRequest(
        cpu = (1, 1),
        memory = ra3.Utils.guessMemoryUsageInMB(input) + ra3.Utils
          .guessMemoryUsageInMB(idx),
        scratch = 0,
        gpu = 0
      )
    ).map(_.asInstanceOf[input.type])
  // $COVERAGE-OFF$
  implicit val codec: JsonValueCodec[TakeIndex] = JsonCodecMaker.make
  implicit val codecOut: JsonValueCodec[Segment] = JsonCodecMaker.make
  // $COVERAGE-ON$
  val task = Task[TakeIndex, Segment]("take", 1) { case input =>
    implicit ce =>
      scribe.debug("Start TakeIndex")
      val bI = input.idx.buffer
      val bIn: IO[input.input.tag.BufferType] =
        input.input.tag.buffer(input.input.segment)
      IO.both(bI, bIn).flatMap { case (idx, in) =>
        input.input.tag
          .toSegment(input.input.tag.take(in, idx), input.outputPath)
      }

  }
}
