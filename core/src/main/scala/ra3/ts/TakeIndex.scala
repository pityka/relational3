package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

case class TakeIndex(
    input: Segment,
    idx: SegmentInt,
    outputPath: LogicalPath
)
object TakeIndex {
  def queue(
      input: Segment,
      idx: SegmentInt,
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ): IO[input.SegmentType] =
    task(TakeIndex(input, idx, outputPath))(
      ResourceRequest(cpu = (1, 1), memory = ra3.Utils.guessMemoryUsageInMB(input)+ra3.Utils.guessMemoryUsageInMB(idx), scratch = 0, gpu = 0)
    ).map(_.as(input))
  implicit val codec: JsonValueCodec[TakeIndex] = JsonCodecMaker.make
  implicit val codecOut: JsonValueCodec[Segment] = JsonCodecMaker.make
  val task = Task[TakeIndex, Segment]("take", 1) { case input =>
    implicit ce =>
      val bI = input.idx.buffer
      val bIn: IO[Buffer] = input.input.buffer
      IO.both(bI, bIn).flatMap { case (idx, in) =>
        in.take(idx).toSegment(input.outputPath)
      }

  }
}
