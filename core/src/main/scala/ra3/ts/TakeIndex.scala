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
  def queue[D <: DataType](
      input: D#SegmentType,
      idx: SegmentInt,
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ): IO[D#SegmentType] =
    task(TakeIndex(input, idx, outputPath))(
      ResourceRequest(cpu = (1, 1), memory = 1, scratch = 0, gpu = 0)
    ).map(_.as[D])
  implicit val codec: JsonValueCodec[TakeIndex] = JsonCodecMaker.make
  val task = Task[TakeIndex, Segment]("take", 1) { case input =>
    implicit ce =>
      val bI = input.idx.buffer
      val bIn: IO[Buffer] = input.input.buffer
      IO.both(bI, bIn).flatMap { case (idx, in) =>
        in.take(idx).toSegment(input.outputPath)
      }

  }
}
