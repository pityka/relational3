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
    def emptyOverlap() = {
      val rightC = right.as(left)
      val rMM = rightC.minMax
      val lMM = left.minMax
      if (rMM.isDefined && lMM.isDefined) {
        val rMin = rMM.get._1
        val rMax = rMM.get._2
        val lMin = lMM.get._1
        val lMax = lMM.get._2
        left.tag.ordering.lt(rMax, lMin) || left.tag.ordering.lt(lMax, rMin)
      } else false
    }
    if (how == "inner" && emptyOverlap())
      IO.pure(
        (Some(ColumnTag.I32.emptySegment), Some(ColumnTag.I32.emptySegment))
      )
    else {

      val bufferedLeft = IO
        .parSequenceN(32)(left.segments.map(_.buffer))
        .map(b => left.tag.cat(b:_*))
      val bufferedRight = IO
        .parSequenceN(32)(right.segments.map(_.buffer))
        .map(b => right.tag.cat(b:_*))

      IO.both(bufferedLeft, bufferedRight).flatMap {
        case (bufferedLeft, bufferedRight) =>
          val (takeLeft, takeRight) =
            bufferedLeft.computeJoinIndexes(bufferedRight.as(bufferedLeft), how)

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
  }
  def queue[C <: Column](
      left: C,
      right: C,
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
