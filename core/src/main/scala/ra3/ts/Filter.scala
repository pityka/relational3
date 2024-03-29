package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

private[ra3] case class Filter(
    input: Segment,
    predicate: Segment,
    outputPath: LogicalPath
)
private[ra3] object Filter {
  def queue(input: Segment, predicate: Segment, outputPath: LogicalPath)(
      implicit tsc: TaskSystemComponents
  ): IO[input.SegmentType] =
    task(Filter(input, predicate, outputPath))(
      ResourceRequest(
        cpu = (1, 1),
        memory = ra3.Utils.guessMemoryUsageInMB(input),
        scratch = 0,
        gpu = 0
      )
    ).map(_.as(input))
  implicit val codec: JsonValueCodec[Filter] = JsonCodecMaker.make
  implicit val codecOut: JsonValueCodec[Segment] = JsonCodecMaker.make
  val task = Task[Filter, Segment]("filter", 1) { case input =>
    implicit ce =>
      val bI: IO[Buffer] = input.predicate.buffer
      val bIn: IO[Buffer] = input.input.buffer
      IO.both(bI, bIn).flatMap { case (predicate, in) =>
        in.filter(predicate).toSegment(input.outputPath)
      }

  }
}
