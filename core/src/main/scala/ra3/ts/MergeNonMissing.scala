package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

case class MergeNonMissing(
    inputs: SegmentPair[_ <: DataType],
    outputPath: LogicalPath
)
object MergeNonMissing {
  def doit(
      pair: SegmentPair[_ <: DataType],
      outputPath: LogicalPath
  )(implicit tsc: TaskSystemComponents) = {
    val a = pair.a.buffer
    val b = pair.b.buffer
    IO.both(a, b).flatMap { case (a, b) =>
      a.mergeNonMissing(b).toSegment(outputPath)
    }
  }
  def queue[D <: DataType](tpe: D)(
      input1: tpe.SegmentType,
      input2: tpe.SegmentType,
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ): IO[tpe.SegmentType] = {
    val pair = tpe.pair(input1, input2)

    task(MergeNonMissing(pair, outputPath))(
      ResourceRequest(cpu = (1, 1), memory = 1, scratch = 0, gpu = 0)
    ).map(_.as[tpe.type])
  }
  implicit val codec: JsonValueCodec[MergeNonMissing] = JsonCodecMaker.make
  val task = Task[MergeNonMissing, Segment[_]]("mergenonmissing", 1) {
    case input =>
      implicit ce => doit(input.inputs, input.outputPath)

  }
}
