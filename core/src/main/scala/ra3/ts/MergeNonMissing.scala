package ra3.ts

import ra3.*
import tasks.*
import tasks.jsonitersupport.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import cats.effect.IO

private[ra3] case class MergeNonMissing(
    tag: ColumnTag,
    inputA: Segment,
    inputB: Segment,
    outputPath: LogicalPath
)
private[ra3] object MergeNonMissing {
  def doit(tag: ColumnTag)(
      inputA: tag.SegmentType,
      inputB: tag.SegmentType,
      outputPath: LogicalPath
  )(implicit tsc: TaskSystemComponents) = {
    val a = tag.buffer(inputA)
    val b = tag.buffer(inputB)
    IO.both(a, b).flatMap { case (a, b) =>
      tag.toSegment(tag.mergeNonMissing(a, b), outputPath)
    }
  }
  def queue(tpe: ColumnTag)(
      input1: tpe.SegmentType,
      input2: tpe.SegmentType,
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ): IO[tpe.SegmentType] = {

    task(MergeNonMissing(tpe, input1, input2, outputPath))(
      ResourceRequest(
        cpu = (1, 1),
        memory = ra3.Utils.guessMemoryUsageInMB(input1) + ra3.Utils
          .guessMemoryUsageInMB(input2),
        scratch = 0,
        gpu = 0
      )
    ).map((_: Segment).asInstanceOf[tpe.SegmentType])
  }
  implicit val codec: JsonValueCodec[MergeNonMissing] = JsonCodecMaker.make
  implicit val codecOut: JsonValueCodec[Segment] = JsonCodecMaker.make
  val task = Task[MergeNonMissing, Segment]("mergenonmissing", 1) {
    case input =>
      implicit ce =>
        doit(input.tag)(
          input.inputA.asInstanceOf[input.tag.SegmentType],
          input.inputB.asInstanceOf[input.tag.SegmentType],
          input.outputPath
        )

  }
}
