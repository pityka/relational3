package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

case class ReductionSum(
    input: Seq[Segment],
    map: SegmentInt,
    numGroups: Int,
    outputPath: LogicalPath
)
object ReductionSum extends ReductionOp {

  override def reduce[S <: Segment { type SegmentType = S }](
      segment: Seq[Segment],
      groupMap: Segment.GroupMap,
      path: LogicalPath
  )(implicit tsc: TaskSystemComponents): IO[S] = {
    queue(segment, groupMap.map, groupMap.numGroups, path)
  }

  override def id: String = "sum"

  def queue[S <: Segment { type SegmentType = S }](
      input: Seq[Segment],
      map: SegmentInt,
      numGroups: Int,
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ): IO[S] =
    task(
      ReductionSum(
        input = input,
        map = map,
        numGroups = numGroups,
        outputPath = outputPath
      )
    )(
      ResourceRequest(
        cpu = (1, 1),
        memory = input.map(ra3.Utils.guessMemoryUsageInMB).sum,
        scratch = 0,
        gpu = 0
      )
    ).map(_.as[S])

  private def doit(
      input: Seq[Segment],
      map: SegmentInt,
      numGroups: Int,
      outputPath: LogicalPath
  )(implicit tsc: TaskSystemComponents) = {
    val parts = map.buffer
    val tag = input.head.tag
    val bIn = IO
      .parSequenceN(32)(input.map(_.as(tag).buffer.map(_.asBufferType)))
      .map(b => tag.cat(b: _*))
    IO.both(parts, bIn).flatMap { case (partitionMap, in) =>
      in.sumGroups(partitionMap, numGroups).toSegment(outputPath)
    }
  }
  implicit val codec: JsonValueCodec[ReductionSum] = JsonCodecMaker.make
  implicit val codec2: JsonValueCodec[Segment] = JsonCodecMaker.make
  val task = Task[ReductionSum, Segment]("ReductionSum", 1) { case input =>
    implicit ce =>
      doit(input.input, input.map, input.numGroups, input.outputPath)

  }
}
