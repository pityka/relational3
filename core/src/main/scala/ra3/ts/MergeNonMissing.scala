package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO



case class MergeNonMissing(
    inputs: SegmentPair[_ <: Segment[_]],
    outputPath: LogicalPath
)
object MergeNonMissing {
  def queue[ S <: Segment[_]](
      input1: S,
      input2: S,
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ) = {
    val pair = input1 match {
      case t: SegmentDouble => F64Pair(t, input2.asInstanceOf[SegmentDouble])
      case t: SegmentInt    => I32Pair(t, input2.asInstanceOf[SegmentInt])
    }
    task(MergeNonMissing(pair, outputPath))(
      ResourceRequest(cpu = (1, 1), memory = 1, scratch = 0, gpu = 0)
    )
  }
  implicit val codec: JsonValueCodec[MergeNonMissing] = JsonCodecMaker.make
  val task = Task[MergeNonMissing, Segment[_]]("mergenonmissing", 1) {
    case input =>
      implicit ce =>
        val a = input.inputs.a.buffer
        val b = input.inputs.b.buffer
        IO.both(a, b).flatMap { case (a, b) =>
          a.mergeNonMissing(b).toSegment(input.outputPath)
        }

  }
}
