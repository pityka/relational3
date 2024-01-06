package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

case class FilterInequality(
    comparisonSegmentAndCutoff: SegmentPair[_ <: DataType],
    input: Segment[_],
    outputPath: LogicalPath,
    lessThan: Boolean
)
object FilterInequality {
  def queue[D <: DataType, D2 <: DataType](
      tpe: D
  )(
      comparisonSegment: tpe.SegmentType,
      input: D2#SegmentType,
      cutoff: tpe.SegmentType,
      outputPath: LogicalPath,
      lessThan: Boolean
  )(implicit
      tsc: TaskSystemComponents
  ) = {

    val pair = tpe.pair(comparisonSegment, cutoff)

    task(
      FilterInequality(pair, input, outputPath, lessThan)
    )(
      ResourceRequest(cpu = (1, 1), memory = 1, scratch = 0, gpu = 0)
    ).map(_.as[D2])
  }

  private def doit(
      comparisonSegmentAndCutoff: SegmentPair[_ <: DataType],
      input: Segment[_],
      outputPath: LogicalPath,
      lessThan: Boolean
  )(implicit tsc: TaskSystemComponents) = {
    val cutoffBuffer = comparisonSegmentAndCutoff.b.buffer
    val comparisonBuffer = comparisonSegmentAndCutoff.a.buffer
    val inputBuffer: IO[Buffer[_ <: DataType]] = input.buffer
    IO.both(IO.both(cutoffBuffer, comparisonBuffer), inputBuffer).flatMap {
      case ((cutoff, comparisonBuffer), inputBuffer) =>
        inputBuffer
          .filterInEquality(cutoff.dType)(cutoff, comparisonBuffer, lessThan)
          .toSegment(outputPath)
    }
  }

  implicit val codec: JsonValueCodec[FilterInequality] = JsonCodecMaker.make
  val task = Task[FilterInequality, Segment[_]]("FilterInequality", 1) {
    case input =>
      implicit ce =>
        // could be shortcut by storing min/max statistics in the segment
        doit(
          input.comparisonSegmentAndCutoff,
          input.input,
          input.outputPath,
          input.lessThan
        )

  }
}
