package ra3.ts

import ra3.*
import tasks.*
import tasks.jsonitersupport.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import cats.effect.IO

private[ra3] case class ExtractGroups(
    tag: ColumnTag,
    input: Seq[Segment],
    map: SegmentInt,
    numGroups: Int,
    outputPath: LogicalPath
)
private[ra3] object ExtractGroups {
  def queue(tag: ColumnTag)(
      input: Seq[tag.SegmentType],
      map: SegmentInt,
      numGroups: Int,
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ): IO[Seq[tag.SegmentType]] =
    IO {
      scribe.debug(
        s"Queuing ExtractGroups with numGroups=$numGroups map size=${map.numElems}, segment type $tag"
      )
    } *> task(
      ExtractGroups(
        tag = tag,
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
    ).map(_.map((_: Segment).asInstanceOf[tag.SegmentType]))

  private def doit(tag: ColumnTag)(
      input: Seq[tag.SegmentType],
      map: SegmentInt,
      numGroups: Int,
      outputPath: LogicalPath
  )(implicit tsc: TaskSystemComponents) = {
    scribe.debug(
      s"ExtractGroups on $numGroups groups with map size ${map.numElems}"
    )
    val parts = map.buffer
    val bIn = IO
      .parSequenceN(32)(input.map(s => tag.buffer(s)))
      .map(b => tag.cat(b*))
    IO.both(parts, bIn).flatMap { case (partitionMap, in) =>
      IO.parSequenceN(32)((0 until numGroups).toList.map { gIdx =>
        tag.toSegment(
          tag
            .take(in, partitionMap.where(gIdx)),
          outputPath.copy(table = outputPath.table + "-g" + gIdx)
        )
      })
    }.logElapsed
  }
  // $COVERAGE-OFF$
  implicit val codec: JsonValueCodec[ExtractGroups] = JsonCodecMaker.make
  implicit val codec2: JsonValueCodec[Seq[Segment]] = JsonCodecMaker.make
  // $COVERAGE-ON$
  val task = Task[ExtractGroups, Seq[Segment]]("ExtractGroups", 1) {
    case input =>
      implicit ce =>
        doit(input.tag)(
          input.input.map((_: Segment).asInstanceOf[input.tag.SegmentType]),
          input.map,
          input.numGroups,
          input.outputPath
        )

  }
}
