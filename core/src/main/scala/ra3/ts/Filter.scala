package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

case class Filter(
    input: Segment,
    predicate: Segment,
    outputPath: LogicalPath
)
object Filter {
  def queue[D<:DataType](input: D#SegmentType, predicate: Segment, outputPath: LogicalPath)(
      implicit tsc: TaskSystemComponents
  ): IO[D#SegmentType] =
    task(Filter(input, predicate, outputPath))(
      ResourceRequest(cpu = (1, 1), memory = 1, scratch = 0, gpu = 0)
    ).map(_.as[D])
  implicit val codec: JsonValueCodec[Filter] = JsonCodecMaker.make
  val task = Task[Filter, Segment]("filter", 1) { case input =>
    implicit ce =>
      val bI : IO[Buffer]= input.predicate.buffer
      val bIn: IO[Buffer] = input.input.buffer
      IO.both(bI, bIn).flatMap { case (predicate, in) =>
        in.filter(predicate).toSegment(input.outputPath)
      }

  }
}