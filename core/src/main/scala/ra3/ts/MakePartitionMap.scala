package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

private[ra3] case class MakePartitionMap(
    input: Vector[Segment],
    outputPath: LogicalPath,
    partitionBase: Int
)
private[ra3] object MakePartitionMap {
  // wrapper is needed so that we avoid having Codec[SegmentInt] in this scope
  // for some reason jsoniter would use that for the codec of MakePartitionMap
  case class Output(segment: SegmentInt)
  def queue(
      input: Vector[Segment],
      partitionBase: Int,
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ): IO[SegmentInt] =
    task(MakePartitionMap(input, outputPath, partitionBase))(
      ResourceRequest(
        cpu = (1, 1),
        memory = input.map(ra3.Utils.guessMemoryUsageInMB).sum,
        scratch = 0,
        gpu = 0
      )
    ).map(_.segment)
  implicit val codec: JsonValueCodec[MakePartitionMap] = JsonCodecMaker.make
  implicit val c2: JsonValueCodec[Output] = JsonCodecMaker.make
  val task = Task[MakePartitionMap, Output]("makepartitionmap", 1) {
    case input =>
      implicit ce =>
        scribe.debug(
          s"Make partition map on ${input.input.size}  segments with base ${input.partitionBase} to ${input.outputPath}"
        )
        val b: IO[Vector[Buffer]] = IO.parSequenceN(
          math.min(1, ce.resourceAllocated.cpu)
        )(input.input.map(_.buffer))
        b.flatMap { in =>
          Buffer
            .computePartitions(buffers = in, num = input.partitionBase)
            .toSegment(input.outputPath)
            .map(Output(_))
        }

  }
}
