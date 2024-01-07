package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

case class FilterInequality(
    comparison: Segment,
    cutoff: Segment,
    input: Segment,
    outputPath: LogicalPath,
    lessThan: Boolean
)
object FilterInequality {
  def queue(
      comparison: Segment,
      input: Segment,
      cutoff: Segment,
      outputPath: LogicalPath,
      lessThan: Boolean
  )(implicit
      tsc: TaskSystemComponents
  ) = {

    task(
      FilterInequality(comparison, cutoff, input, outputPath, lessThan)
    )(
      ResourceRequest(cpu = (1, 1), memory = 1, scratch = 0, gpu = 0)
    ).map(_.as(input))
  }

  private def doit(
      comparison: Segment,
      cutoff: Segment,
      input: Segment,
      outputPath: LogicalPath,
      lessThan: Boolean
  )(implicit tsc: TaskSystemComponents) = {

    val inputBuffer: IO[Buffer] = input.buffer
    IO.both(IO.both(cutoff.buffer, comparison.buffer), inputBuffer).flatMap {
      case ((cutoff, comparisonBuffer), inputBuffer) =>
        inputBuffer
          .filterInEquality[
            comparison.Elem,
            comparison.BufferType,
            comparison.BufferType
          ](comparisonBuffer, cutoff.as(comparisonBuffer), lessThan)
          .toSegment(outputPath)
    }
  }

  implicit val codec: JsonValueCodec[FilterInequality] = JsonCodecMaker.make
  val task = Task[FilterInequality, Segment]("FilterInequality", 1) {
    case input =>
      implicit ce =>
        // could be shortcut by storing min/max statistics in the segment
        doit(
          input.comparison,
          input.cutoff,
          input.input,
          input.outputPath,
          input.lessThan
        )

  }
}
