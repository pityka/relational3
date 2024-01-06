package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

case class EstimateCDF(
    input: Segment[_<:DataType],
    numberOfPoints: Int,
    outputPath: LogicalPath
)
object EstimateCDF {
  private def doit[D <: DataType](
      buffer: D#BufferType,
      n: Int,
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ) = {
    val t = buffer.cdf(n)
    IO.both(
      t._1.toSegment(outputPath.appendToTable(".cdfX")),
      t._2
        .toSegment(outputPath.appendToTable(".cdfY"))        
    )
  }
  def queue[D <: DataType](
      input: D#SegmentType,
      numberOfPoints: Int,
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ): IO[(D#SegmentType, SegmentInt)] =
    task(EstimateCDF(input, numberOfPoints, outputPath))(
      ResourceRequest(cpu = (1, 1), memory = 1, scratch = 0, gpu = 0)
    ).map(pair => (pair._1.as[D],pair._2))
  implicit val codec: JsonValueCodec[EstimateCDF] = JsonCodecMaker.make
  implicit val code2: JsonValueCodec[(Segment[_], SegmentInt)] =
    JsonCodecMaker.make
  val task = Task[EstimateCDF, (Segment[_], SegmentInt)]("estimatecdf", 1) {
    case input =>
      implicit ce =>
        val bIn = input.input.buffer
        bIn.flatMap { case bIn =>
          doit(bIn, input.numberOfPoints, input.outputPath)
        }

  }
}
