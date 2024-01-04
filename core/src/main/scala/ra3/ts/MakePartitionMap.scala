package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

case class MakePartitionMap(
    input: Seq[Segment[_]],
    outputPath: LogicalPath,
    numPartitions: Int
)
object MakePartitionMap {
  def queue(
      input: Seq[Segment[_]],
      numPartitions: Int,
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ) =
    task(MakePartitionMap(input, outputPath, numPartitions))(
      ResourceRequest(cpu = (1, 1), memory = 1, scratch = 0, gpu = 0)
    ).map(_.asInstanceOf[SegmentInt])
  implicit val codec: JsonValueCodec[MakePartitionMap] = JsonCodecMaker.make
  val task = Task[MakePartitionMap, Segment[_]]("makepartitionmap", 1) {
    case input =>
      implicit ce =>
        val b: IO[Seq[Buffer[_]]] = IO.parSequenceN(
          math.min(1, ce.resourceAllocated.cpu)
        )(input.input.map(_.buffer))
        b.flatMap { in =>
          Buffer
            .computePartitions(in, input.numPartitions)
            .toSegment(input.outputPath)
        }

  }
}
