package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

case class EstimateCDF(
    input: Segment,
    numberOfPoints: Int,
    outputPath: LogicalPath
)
object EstimateCDF {
  private def doit(
      input: Segment,
      n: Int,
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ) = {
    val bIn = input.buffer
    bIn.flatMap { case bIn =>
      val t = bIn.cdf(n)
      IO.both(
        t._1.toSegment(outputPath.appendToTable(".cdfX")),
        t._2
          .toSegment(outputPath.appendToTable(".cdfY"))
      )
    }
  }
  def queue(
      input: Segment,
      numberOfPoints: Int,
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ): IO[(input.SegmentType, SegmentInt)] =
    task(EstimateCDF(input, numberOfPoints, outputPath))(
      ResourceRequest(cpu = (1, 1), memory = 1, scratch = 0, gpu = 0)
    ).map(pair => (pair._1.as(input), pair._2))
  implicit val codec: JsonValueCodec[EstimateCDF] = JsonCodecMaker.make
  implicit val code2: JsonValueCodec[(Segment, SegmentInt)] =
    JsonCodecMaker.make
  val task = Task[EstimateCDF, (Segment, SegmentInt)]("estimatecdf", 1) {
    case input =>
      implicit ce => doit(input.input, input.numberOfPoints, input.outputPath)

  }
}
