package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

case class TakePartition(
    input: Segment[_],
    partitionMap: SegmentInt,
    pIdx: Int,
    outputPath: LogicalPath
)
object TakePartition {
  def queue(
      input: Segment[_],
      partitionMap: SegmentInt,
      outputPath: LogicalPath,
      pIdx: Int
  )(implicit
      tsc: TaskSystemComponents
  ) =
    task(
      TakePartition(
        input = input,
        partitionMap = partitionMap,
        pIdx = pIdx,
        outputPath = outputPath
      )
    )(
      ResourceRequest(cpu = (1, 1), memory = 1, scratch = 0, gpu = 0)
    )
  implicit val codec: JsonValueCodec[TakePartition] = JsonCodecMaker.make
  val task = Task[TakePartition, Segment[_]]("takepartition", 1) { case input =>
    implicit ce =>
      val parts = input.partitionMap.buffer
      val bIn: IO[Buffer[_]] = input.input.buffer
      IO.both(parts, bIn).flatMap { case (partitionMap, in) =>
        in
          .take(partitionMap.where(input.pIdx))
          .toSegment(
            input.outputPath
          )
      }

  }
}
