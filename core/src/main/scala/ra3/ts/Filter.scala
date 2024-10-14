package ra3.ts

import ra3.*
import tasks.*
import tasks.jsonitersupport.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import cats.effect.IO

private[ra3] case class Filter(
    input: TaggedSegment,
    predicate: TaggedSegment,
    outputPath: LogicalPath
)
private[ra3] object Filter {
  def queue(tag: ColumnTag, predicateTag: ColumnTag)(
      input: tag.SegmentType,
      predicate: predicateTag.SegmentType,
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ): IO[tag.SegmentType] =
    task(
      Filter(
        tag.makeTaggedSegment(input),
        predicateTag.makeTaggedSegment(predicate),
        outputPath
      )
    )(
      ResourceRequest(
        cpu = (1, 1),
        memory = ra3.Utils.guessMemoryUsageInMB(input),
        scratch = 0,
        gpu = 0
      )
    ).map(_.asInstanceOf[input.type])
    // $COVERAGE-OFF$
  implicit val codec: JsonValueCodec[Filter] = JsonCodecMaker.make
  implicit val codecOut: JsonValueCodec[Segment] = JsonCodecMaker.make
    // $COVERAGE-ON$
  val task = Task[Filter, Segment]("filter", 1) { case input =>
    implicit ce =>
      val tag = input.input.tag
      val bI: IO[input.predicate.tag.BufferType] =
        input.predicate.tag.buffer(input.predicate.segment)
      val bIn: IO[tag.BufferType] = tag.buffer(input.input.segment)
      IO.both(bI, bIn).flatMap { case (predicate, in) =>
        tag.toSegment(tag.filter(in, predicate), input.outputPath)
      }

  }
}
