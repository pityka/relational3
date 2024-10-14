package ra3.ts

import ra3.*
import tasks.*
import tasks.jsonitersupport.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import cats.effect.IO

private[ra3] case class MakePartitionMap(
    input: Vector[TaggedSegment],
    outputPath: LogicalPath,
    partitionBase: Int
)
private[ra3] object MakePartitionMap {
  // wrapper is needed so that we avoid having Codec[SegmentInt] in this scope
  // for some reason jsoniter would use that for the codec of MakePartitionMap
  case class Output(segment: SegmentInt)
  def queue(
      input: Vector[TaggedSegment],
      partitionBase: Int,
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ): IO[SegmentInt] =
    task(MakePartitionMap(input, outputPath, partitionBase))(
      ResourceRequest(
        cpu = (1, 1),
        memory = input.map(s => ra3.Utils.guessMemoryUsageInMB(s.segment)).sum,
        scratch = 0,
        gpu = 0
      )
    ).map(_.segment)
    // $COVERAGE-OFF$
  implicit val codec: JsonValueCodec[MakePartitionMap] = JsonCodecMaker.make
  implicit val c2: JsonValueCodec[Output] = JsonCodecMaker.make
    // $COVERAGE-ON$
  val task = Task[MakePartitionMap, Output]("makepartitionmap", 1) {
    case input =>
      implicit ce =>
        scribe.debug(
          s"Make partition map on ${input.input.size}  segments with base ${input.partitionBase} to ${input.outputPath}"
        )
        val b: IO[Vector[Buffer]] = IO.parSequenceN(
          math.min(1, ce.resourceAllocated.cpu)
        )(input.input.map(s => s.tag.buffer(s.segment)))
        b.flatMap { in =>
          ColumnTag.I32
            .toSegment(
              Buffer
                .computePartitions(buffers = in, num = input.partitionBase),
              input.outputPath
            )
            .map(Output(_))
        }

  }
}
