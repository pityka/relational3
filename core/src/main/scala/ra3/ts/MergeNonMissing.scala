package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

case class MergeNonMissing(
    input1: Segment[_],
    input2: Segment[_],    
    outputPath: LogicalPath
)
object MergeNonMissing {
  def queue(input1: Segment[_], input2: Segment[_], outputPath: LogicalPath)(
      implicit tsc: TaskSystemComponents
  ) =
    task(MergeNonMissing(input1, input2, outputPath))(
      ResourceRequest(cpu = (1, 1), memory = 1, scratch = 0, gpu = 0)
    )
  implicit val codec: JsonValueCodec[MergeNonMissing] = JsonCodecMaker.make
  val task = Task[MergeNonMissing, Segment[_]]("mergenonmissing", 1) { case input =>
    implicit ce =>
      val a: IO[Buffer[_]] = input.input1.buffer
      val b: IO[Buffer[_]] = input.input2.buffer
      IO.both(a,b).flatMap { case (a,b) =>
        a.mergeNonMissing(b).toSegment(input.outputPath)
      }

  }
}