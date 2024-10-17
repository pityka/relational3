package ra3.ts

import ra3.*
import tasks.*
import tasks.jsonitersupport.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import cats.effect.IO

private[ra3] case class MakeGroupMap(
    input: Seq[TaggedColumn],
    outputPath: LogicalPath
)
private[ra3] object MakeGroupMap {

  private def singleColumn(tag: ColumnTag)(
      column: tag.ColumnType
  )(implicit tsc: TaskSystemComponents): IO[tag.TaggedBuffersType] = {
    val z = tag.segments(column).map(s => tag.buffer(s))
    val t = IO.parSequenceN(32)(z)
    t.map(tag.makeTaggedBuffers)
  }

  private def doit(input: Seq[TaggedColumn], outputPath: LogicalPath)(implicit
      tsc: TaskSystemComponents
  ) = {
    scribe.debug(s"Make group map on $input to $outputPath")
    assert(input.map(_.tag).distinct.size == 1)
    val bufferedColumns: IO[Seq[TaggedBuffers]] =
      IO.parSequenceN(32)(input.map { column =>
        singleColumn(column.tag)(column.column)
      })

    bufferedColumns.flatMap { case taggedBuffers =>
      import Buffer.GroupMap
      val GroupMap(groupMap, numGroups, groupSizes) = Buffer
        .computeGroups(taggedBuffers)

      val intTag = ColumnTag.I32

      IO.both(
        intTag.toSegment(groupMap, outputPath),
        intTag.toSegment(
          groupSizes,
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
      input: Seq[TaggedColumn],
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ) =
  task(MakeGroupMap(input, outputPath))(
    ResourceRequest(
      cpu = (1, 1),
      memory = input.map(ra3.Utils.guessMemoryUsageInMB).sum * 8,
      scratch = 0,
      gpu = 0
    )
  ).map { case (map, numberOfGroups, sizes) =>
    (map, numberOfGroups, sizes)
  }
  // $COVERAGE-OFF$
  implicit val codec: JsonValueCodec[MakeGroupMap] = JsonCodecMaker.make
  implicit val codec2: JsonValueCodec[(SegmentInt, Int, SegmentInt)] =
  JsonCodecMaker.make
  // $COVERAGE-ON$

  val task =
    Task[MakeGroupMap, (SegmentInt, Int, SegmentInt)]("MakeGroupMap", 1) {
      case input =>
        implicit ce => doit(input.input, input.outputPath)

    }
}
