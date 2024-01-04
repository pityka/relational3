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

  /** Returns (group map, num groups, sizes of groups)
    */
  def queue(
      input: Seq[Column],
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ) =
    task(MakeGroupMap(input, outputPath))(
      ResourceRequest(cpu = (1, 1), memory = 1, scratch = 0, gpu = 0)
    ).map { case (segment, numberOfGroups, sizes) =>
      (segment, numberOfGroups, sizes)
    }

  implicit val codec: JsonValueCodec[MakeGroupMap] = JsonCodecMaker.make
  implicit val codec2: JsonValueCodec[(SegmentInt, Int, SegmentInt)] =
    JsonCodecMaker.make

  val task =
    Task[MakeGroupMap, (SegmentInt, Int, SegmentInt)]("MakeGroupMap", 1) {
      case input =>
        implicit ce =>
          val bufferedColumns = IO.parSequenceN(32)(input.input.map { column =>
            IO
              .parSequenceN(32)(column.segments.map(_.buffer))
              .map(_.reduce(_ ++ _))
          })

          bufferedColumns.flatMap { in =>
            import Buffer.GroupMap
            val GroupMap(groupMap, numGroups, groupSizes) = Buffer
              .computeGroups(in)

            IO.both(
              groupMap.toSegment(input.outputPath),
              groupSizes.toSegment(
                input.outputPath
                  .copy(table = input.outputPath.table + ".groupsizes")
              )
            ).map { case (a, b) => (a match {
              case x :SegmentInt =>  x
              
            }, numGroups, b match {
              case x: SegmentInt =>  x
              
            }) }
          }

    }
}
