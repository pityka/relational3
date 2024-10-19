package ra3.ts

import ra3.*
import tasks.*
import tasks.jsonitersupport.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import cats.effect.IO

private[ra3] case class EstimateCDF(
    input: TaggedSegment,
    numberOfPoints: Int,
    outputPath: LogicalPath
)
private[ra3] object EstimateCDF {
  private def doit(
      input: TaggedSegment,
      n: Int,
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ) = {
    scribe.debug(
      f"estimate cdf input=${input.tag}(numEl=${input.segment.numElems}%,d) sampling points=$n%,d to $outputPath "
    )
    val tag = input.tag
    val doubleTag = ColumnTag.F64
    val bIn = tag.buffer(input.segment)
    bIn.flatMap { case bIn =>
      val t = tag.cdf(bIn, n)
      IO.both(
        tag.toSegment(t._1, outputPath.appendToTable(".cdfX")),
        doubleTag.toSegment(t._2, outputPath.appendToTable(".cdfY"))
      )
    }
  }
  def queue(tag: ColumnTag)(
      input: tag.SegmentType,
      numberOfPoints: Int,
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ): IO[(tag.SegmentType, SegmentDouble)] =
    IO {
      scribe.debug(
        s"Queueing EstimateCDF of type $tag with items ${tag.numElems(input)}"
      )
    } *> task(
      EstimateCDF(tag.makeTaggedSegment(input), numberOfPoints, outputPath)
    )(
      ResourceRequest(
        cpu = (1, 1),
        memory = ra3.Utils.guessMemoryUsageInMB(input),
        scratch = 0,
        gpu = 0
      )
    ).map(pair => (pair._1.asInstanceOf[input.type], pair._2))
  // $COVERAGE-OFF$
  implicit val codec: JsonValueCodec[EstimateCDF] = JsonCodecMaker.make
  implicit val code2: JsonValueCodec[(Segment, SegmentDouble)] =
    JsonCodecMaker.make
  // $COVERAGE-ON$
  val task = Task[EstimateCDF, (Segment, SegmentDouble)]("estimatecdf", 1) {
    case input =>
      implicit ce => doit(input.input, input.numberOfPoints, input.outputPath)

  }
}
