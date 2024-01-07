package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

case class MakePartitionMap(
    input: Seq[Segment],
    outputPath: LogicalPath,
    numPartitions: Int
)
object MakePartitionMap {
  // wrapper is needed so that we avoid having Codec[SegmentInt] in this scope
  // for some reason jsoniter would use that for the codec of MakePartitionMap 
  case class Output(segment: SegmentInt)
  def queue(
      input: Seq[Segment],
      numPartitions: Int,
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ): IO[SegmentInt] =
    task(MakePartitionMap(input, outputPath, numPartitions))(
      ResourceRequest(cpu = (1, 1), memory = 1, scratch = 0, gpu = 0)
    ).map(_.segment)
  implicit val codec: JsonValueCodec[MakePartitionMap] = JsonCodecMaker.make
  implicit val c2: JsonValueCodec[Output] = JsonCodecMaker.make
  val task = Task[MakePartitionMap, Output]("makepartitionmap", 1) {
    case input =>
      implicit ce =>
        val b: IO[Seq[Buffer]] = IO.parSequenceN(
          math.min(1, ce.resourceAllocated.cpu)
        )(input.input.map(_.buffer))
        b.flatMap { in =>
          Buffer
            .computePartitions(in, input.numPartitions)
            .toSegment(input.outputPath).map(Output(_))
        }

  }
}
