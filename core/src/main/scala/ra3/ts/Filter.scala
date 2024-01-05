package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

case class Filter(
    input: Segment[_],
    predicate: Segment[_],
    outputPath: LogicalPath
)
object Filter {
  def queue[D<:DataType](input: Segment[D], predicate: Segment[_], outputPath: LogicalPath)(
      implicit tsc: TaskSystemComponents
  ) =
    task(Filter(input, predicate, outputPath))(
      ResourceRequest(cpu = (1, 1), memory = 1, scratch = 0, gpu = 0)
    ).map(_ match {
      case t: Segment[_] => t.asInstanceOf[Segment[D]]
    })
  implicit val codec: JsonValueCodec[Filter] = JsonCodecMaker.make
  val task = Task[Filter, Segment[_]]("filter", 1) { case input =>
    implicit ce =>
      val bI : IO[Buffer[_]]= input.predicate.buffer
      val bIn: IO[Buffer[_]] = input.input.buffer
      IO.both(bI, bIn).flatMap { case (predicate, in) =>
        in.filter(predicate).toSegment(input.outputPath)
      }

  }
}