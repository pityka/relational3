package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

case class MakeGroupMap(
    input: Seq[Column],
    outputPath: LogicalPath
)
object MakeGroupMap {

  private def singleColumn(
      column: Column
  )(implicit tsc: TaskSystemComponents) = {
    val z = column.segments.map(_.buffer)
    val t = IO.parSequenceN(32)(z)
    t
  }

  private def doit(input: Seq[Column], outputPath: LogicalPath)(implicit
      tsc: TaskSystemComponents
  ) = {
    scribe.debug(s"Make group map on $input to $outputPath")
    assert(input.map(_.tag).distinct.size == 1)
    val tag = input.head.tag
    val bufferedColumns = IO.parSequenceN(32)(input.map { column =>
      singleColumn(column).map(_.map(_.as(tag)))
    })

    bufferedColumns.flatMap { in =>
      import Buffer.GroupMap
      val GroupMap(groupMap, numGroups, groupSizes) = Buffer
        .computeGroups(in)

      IO.both(
        groupMap.toSegment(outputPath),
        groupSizes.toSegment(
          outputPath
            .copy(table = outputPath.table + ".groupsizes")
        )
      ).map { case (a, b) =>
        (
          a,
          numGroups,
          b
        )
      }
    }

  }

  /** Returns (group map, num groups, sizes of groups)
    */
  def queue(
      input: Seq[Column],
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ) =
    task(MakeGroupMap(input, outputPath))(
      ResourceRequest(cpu = (1, 1), memory = input.map(ra3.Utils.guessMemoryUsageInMB).sum * 8, scratch = 0, gpu = 0)
    ).map { case (map, numberOfGroups, sizes) =>
      (map, numberOfGroups, sizes)
    }

  implicit val codec: JsonValueCodec[MakeGroupMap] = JsonCodecMaker.make
  implicit val codec2: JsonValueCodec[(SegmentInt, Int, SegmentInt)] =
    JsonCodecMaker.make

  val task =
    Task[MakeGroupMap, (SegmentInt, Int, SegmentInt)]("MakeGroupMap", 1) {
      case input =>
        implicit ce => doit(input.input, input.outputPath)

    }
}
