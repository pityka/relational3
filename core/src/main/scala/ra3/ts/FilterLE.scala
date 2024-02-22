package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

private[ra3] case class FilterInequality(
    comparison: Segment,
    cutoff: Segment,
    input: Segment,
    outputPath: LogicalPath,
    lessThan: Boolean
)
private[ra3] object FilterInequality {
  def queue[S <: Segment { type SegmentType = S }](
      comparison: S,
      input: Segment,
      cutoff: S,
      outputPath: LogicalPath,
      lessThan: Boolean
  )(implicit
      tsc: TaskSystemComponents
  ): IO[input.SegmentType] = {

    task(
      FilterInequality(comparison, cutoff, input, outputPath, lessThan)
    )(
      ResourceRequest(
        cpu = (1, 1),
        memory = ra3.Utils.guessMemoryUsageInMB(input),
        scratch = 0,
        gpu = 0
      )
    ).map(_.as(input))
  }

  private def doit(
      comparison: Segment,
      cutoff: Segment,
      input: Segment,
      outputPath: LogicalPath,
      lessThan: Boolean
  )(implicit tsc: TaskSystemComponents) = {
    val cutoffAsSegment = cutoff.as(comparison)
    val nonEmpty = cutoffAsSegment.buffer.map(_.toSeq.head).map { cutoffValue =>
      comparison.nonMissingMinMax
        .map { case (min, max) =>
          if (lessThan) {
            comparison.tag.ordering.gteq(cutoffValue, min)

          } else {
            comparison.tag.ordering.lteq(cutoffValue, max)
          }
        }
        .getOrElse(true)
    }
    nonEmpty.flatMap { nonEmpty =>
      if (nonEmpty)
        IO.both(
          IO.both(cutoffAsSegment.buffer, comparison.buffer),
          input.buffer
        ).flatMap { case ((cutoff, comparisonBuffer), inputBuffer) =>
          inputBuffer
            .filterInEquality[
              comparison.BufferType
            ](comparisonBuffer, cutoff, lessThan)
            .toSegment(outputPath)
        }
      else IO.pure(input.tag.emptySegment)
    }
  }

  implicit val codec: JsonValueCodec[FilterInequality] = JsonCodecMaker.make
  implicit val codecOut: JsonValueCodec[Segment] = JsonCodecMaker.make
  val task = Task[FilterInequality, Segment]("FilterInequality", 1) {
    case input =>
      implicit ce =>
        doit(
          input.comparison,
          input.cutoff,
          input.input,
          input.outputPath,
          input.lessThan
        )

  }
}
