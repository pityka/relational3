package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

case class ExtractGroups(
    input: Segment[_],
    map: SegmentInt,
    numGroups: Int,
    outputPath: LogicalPath
)
object ExtractGroups {
  def queue(
      input: Segment[_],
      map: SegmentInt,
      numGroups: Int,
      outputPath: LogicalPath,
  )(implicit
      tsc: TaskSystemComponents
  ) =
    task(
      ExtractGroups(
        input = input,
        map = map,
        numGroups = numGroups,
        outputPath = outputPath
      )
    )(
      ResourceRequest(cpu = (1, 1), memory = 1, scratch = 0, gpu = 0)
    )
  implicit val codec: JsonValueCodec[ExtractGroups] = JsonCodecMaker.make
  implicit val codec2: JsonValueCodec[Seq[Segment[_]]] = JsonCodecMaker.make
  val task = Task[ExtractGroups, Seq[Segment[_]]]("ExtractGroups", 1) { case input =>
    implicit ce =>
      val parts = input.map.buffer
      val bIn: IO[Buffer[_]] = input.input.buffer
      IO.both(parts, bIn).flatMap { case (partitionMap, in) =>
        IO.parSequenceN(32)((0 until input.numGroups).toList.map{ gIdx =>
        in
          .take(partitionMap.where(gIdx))
          .toSegment(
            input.outputPath.copy(table=input.outputPath.table+"-g"+gIdx)
          )
        })
      }

  }
}
