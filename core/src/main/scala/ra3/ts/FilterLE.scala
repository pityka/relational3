package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

case class FilterInequality(    
    comparisonSegmentAndCutoff: SegmentPair[_ <: Segment[_]],
    input: Segment[_],
    outputPath: LogicalPath,
    lessThan: Boolean
)
object FilterInequality {
  def queue[D<:DataType,D2<:DataType](
      tpe: D
  )(
      comparisonSegment: tpe.SegmentType,
      input: D2#SegmentType,
      cutoff: tpe.SegmentType,
      outputPath: LogicalPath,
      lessThan: Boolean
  )(implicit
      tsc: TaskSystemComponents,
      // ev: comparisonSegment.type =:= cutoff.type
  ) =
    {

      val pair = tpe.pair(comparisonSegment,cutoff)

      task(
      FilterInequality(pair, input, outputPath, lessThan)
    )(
      ResourceRequest(cpu = (1, 1), memory = 1, scratch = 0, gpu = 0)
    ).map(_.as[D2])
    }
  implicit val codec: JsonValueCodec[FilterInequality] = JsonCodecMaker.make
  val task = Task[FilterInequality, Segment[_]]("FilterInequality", 1) {
    case input =>
      implicit ce =>
        // could be shortcut by storing min/max statistics in the segment
        val cutoffBuffer = input.comparisonSegmentAndCutoff.b.buffer
        val comparisonBuffer = input.comparisonSegmentAndCutoff.a.buffer
        val inputBuffer = input.input match {
          // case SegmentDouble(sf, numElems) =>
          case t: SegmentInt => t.buffer
        }
        IO.both(IO.both(cutoffBuffer, comparisonBuffer), inputBuffer).flatMap {
          case ((cutoff, comparisonBuffer), inputBuffer) =>
            inputBuffer
              .filterInEquality(cutoff, comparisonBuffer, input.lessThan)
              .toSegment(input.outputPath)
        }

  }
}
