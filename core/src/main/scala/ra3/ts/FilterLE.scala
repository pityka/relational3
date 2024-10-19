package ra3.ts

import ra3.*
import tasks.*
import tasks.jsonitersupport.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import cats.effect.IO

private[ra3] case class FilterInequality(
    comparisonAndCutoff: TaggedSegments,
    input: TaggedSegment,
    outputPath: LogicalPath,
    lessThan: Boolean
)
private[ra3] object FilterInequality {
  def queue(tag: ColumnTag, comparisonTag: ColumnTag)(
      comparison: comparisonTag.SegmentType,
      input: tag.SegmentType,
      cutoff: comparisonTag.SegmentType,
      outputPath: LogicalPath,
      lessThan: Boolean
  )(implicit
      tsc: TaskSystemComponents
  ): IO[tag.SegmentType] = {
    IO {
      scribe.debug(
        s"Queueing FilterInequaity on ${input.numElems} size of type $tag"
      )
    } *>
      task(
        FilterInequality(
          comparisonTag.makeTaggedSegments(List(comparison, cutoff)),
          tag.makeTaggedSegment(input),
          outputPath,
          lessThan
        )
      )(
        ResourceRequest(
          cpu = (1, 1),
          memory = ra3.Utils.guessMemoryUsageInMB(input),
          scratch = 0,
          gpu = 0
        )
      ).map(_.asInstanceOf[input.type])
  }

  private def doit(cutoffTag: ColumnTag, inputTag: ColumnTag)(
      comparison: cutoffTag.SegmentType,
      cutoff: cutoffTag.SegmentType,
      input: inputTag.SegmentType,
      outputPath: LogicalPath,
      lessThan: Boolean
  )(implicit tsc: TaskSystemComponents) = {
    scribe.debug("FilterLE start")
    val nonEmpty =
      cutoffTag.buffer(cutoff).map(_.toSeq.head).map { cutoffValue =>
        cutoffTag
          .nonMissingMinMax(comparison)
          .map { case (min, max) =>
            if (lessThan) {
              cutoffTag.ordering.gteq(cutoffValue, min)

            } else {
              cutoffTag.ordering.lteq(cutoffValue, max)
            }
          }
          .getOrElse(true)
      }
    nonEmpty.flatMap { nonEmpty =>
      if (nonEmpty)
        IO.both(
          IO.both(cutoffTag.buffer(cutoff), cutoffTag.buffer(comparison)),
          inputTag.buffer(input)
        ).flatMap { case ((cutoffBuffer, comparisonBuffer), inputBuffer) =>
          inputTag.toSegment(
            inputTag
              .filterInEquality(inputBuffer, cutoffTag, lessThan)(
                comparisonBuffer,
                cutoffBuffer
              ),
            outputPath
          )
        }
      else IO.pure(inputTag.emptySegment)
    }
  }
  // $COVERAGE-OFF$
  implicit val codec: JsonValueCodec[FilterInequality] = JsonCodecMaker.make
  implicit val codecOut: JsonValueCodec[Segment] = JsonCodecMaker.make
  // $COVERAGE-ON$
  val task = Task[FilterInequality, Segment]("FilterInequality", 1) {
    case input =>
      implicit ce =>
        doit(input.comparisonAndCutoff.tag, input.input.tag)(
          comparison = input.comparisonAndCutoff.segments(0),
          cutoff = input.comparisonAndCutoff.segments(1),
          input = input.input.segment,
          outputPath = input.outputPath,
          lessThan = input.lessThan
        )

  }
}
