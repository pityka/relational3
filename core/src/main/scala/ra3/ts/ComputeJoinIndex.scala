package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

case class ComputeJoinIndex(
    left: Column,
    right: Column,
    how: String,
    outputPath: LogicalPath
)
object ComputeJoinIndex {
  private def doit(
      left: Column,
      right: Column,
      how: String,
      outputPath: LogicalPath
  )(implicit tsc: TaskSystemComponents) = {
    val rightC = right.asInstanceOf[left.dataType.ColumnType]
    val bufferedLeft = IO
      .parSequenceN(32)(left.segments.map(_.buffer))
      .map(_.reduce(_ ++ _))
    val bufferedRight = IO
      .parSequenceN(32)(rightC.segments.map(_.buffer))
      .map(_.reduce(_ ++ _))

    IO.both(bufferedLeft, bufferedRight).flatMap {
      case (bufferedLeft, bufferedRight) =>
        val (takeLeft, takeRight) =
          bufferedLeft.computeJoinIndexes(bufferedRight, how)

        val takeLeftS: IO[Option[SegmentInt]] =
          takeLeft
            .map(
              _.toSegment(
                outputPath
                  .copy(table = outputPath.table + ".left")
              ).map(_.asInstanceOf[SegmentInt]).map(Some(_))
            )
            .getOrElse(IO.pure(None))
        val takeRightS: IO[Option[SegmentInt]] = takeRight
          .map(
            _.toSegment(
              outputPath
                .copy(table = outputPath.table + ".right")
            ).map(_.asInstanceOf[SegmentInt]).map(Some(_))
          )
          .getOrElse(IO.pure(None))
        IO.both(takeLeftS, takeRightS)
    }
  }
  def queue[D<:DataType](
      left: D#ColumnType,
      right: D#ColumnType,
      how: String,
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ): IO[(Option[SegmentInt], Option[SegmentInt])] =
    task(ComputeJoinIndex(left, right, how, outputPath))(
      ResourceRequest(cpu = (1, 1), memory = 1, scratch = 0, gpu = 0)
    )
  implicit val codec: JsonValueCodec[ComputeJoinIndex] = JsonCodecMaker.make
  implicit val codec2
      : JsonValueCodec[(Option[SegmentInt], Option[SegmentInt])] =
    JsonCodecMaker.make
  val task =
    Task[ComputeJoinIndex, (Option[SegmentInt], Option[SegmentInt])](
      "ComputeJoinIndex",
      1
    ) { case input =>
      implicit ce => doit(input.left, input.right, input.how, input.outputPath)

    }
}
