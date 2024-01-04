package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

case class EstimateCDF(
    input: Segment[_],
    numberOfPoints: Int,
    outputPath: LogicalPath
)
object EstimateCDF {
  private def doit[T](buffer: Buffer[T], n: Int, outputPath: LogicalPath)(
      implicit tsc: TaskSystemComponents
  ) = {
    val t = buffer.cdf(n)
    IO.both(
      t._1.toSegment(outputPath.appendToTable(".cdfX")),
      t._2
        .toSegment(outputPath.appendToTable(".cdfXY")).map(_ match {
          case t: SegmentInt => t
        })
        
    )
  }
  def queue(input: Segment[_], numberOfPoints: Int, outputPath: LogicalPath)(
      implicit tsc: TaskSystemComponents
  ) =
    task(EstimateCDF(input, numberOfPoints, outputPath))(
      ResourceRequest(cpu = (1, 1), memory = 1, scratch = 0, gpu = 0)
    )
  implicit val codec: JsonValueCodec[EstimateCDF] = JsonCodecMaker.make
  implicit val code2: JsonValueCodec[(Segment[_], SegmentInt)] =
    JsonCodecMaker.make
  val task = Task[EstimateCDF, (Segment[_], SegmentInt)]("estimatecdf", 1) {
    case input =>
      implicit ce =>
        val bIn: IO[Buffer[_]] = input.input.buffer
        bIn.flatMap { case bIn =>
          bIn match {
            case t: BufferInt =>
              doit(t, input.numberOfPoints, input.outputPath)
          }
        }

  }
}
