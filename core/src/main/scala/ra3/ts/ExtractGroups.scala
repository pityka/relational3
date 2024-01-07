package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

case class ExtractGroups(
    input: Segment,
    map: SegmentInt,
    numGroups: Int,
    outputPath: LogicalPath
)
object ExtractGroups {
  def queue(
      input: Segment,
      map: SegmentInt,
      numGroups: Int,
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ): IO[Seq[input.SegmentType]] =
    task(
      ExtractGroups(
        input = input,
        map = map,
        numGroups = numGroups,
        outputPath = outputPath
      )
    )(
      ResourceRequest(cpu = (1, 1), memory = 1, scratch = 0, gpu = 0)
    ).map(_.map(_.as(input)))

  private def doit(
      input: Segment,
      map: SegmentInt,
      numGroups: Int,
      outputPath: LogicalPath
  )(implicit tsc: TaskSystemComponents) = {
    val parts = map.buffer
    val bIn = input.buffer
    IO.both(parts, bIn).flatMap { case (partitionMap, in) =>
      IO.parSequenceN(32)((0 until numGroups).toList.map { gIdx =>
        in
          .take(partitionMap.where(gIdx))
          .toSegment(
            outputPath.copy(table = outputPath.table + "-g" + gIdx)
          )
      })
    }
  }
  implicit val codec: JsonValueCodec[ExtractGroups] = JsonCodecMaker.make
  implicit val codec2: JsonValueCodec[Seq[Segment]] = JsonCodecMaker.make
  val task = Task[ExtractGroups, Seq[Segment]]("ExtractGroups", 1) {
    case input =>
      implicit ce =>
        doit(input.input, input.map, input.numGroups, input.outputPath)

  }
}
