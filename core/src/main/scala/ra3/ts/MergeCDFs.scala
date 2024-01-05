package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

case class MergeCDFs(
    inputs: Seq[(Segment[_], SegmentInt)],
    outputPath: LogicalPath
)
object MergeCDFs {

  private def doit(inputs: Seq[(Segment[_],SegmentInt)], outputPath: LogicalPath)(implicit tsc: TaskSystemComponents) = {
     IO.parSequenceN(32)(inputs.map { case (x: Segment[_], y) =>
        IO.both(x.buffer, y.buffer)
      }).flatMap { cdfs =>
        cdfs.head._1 match {
          case _: BufferInt =>
            val (x, y) = Utils
              .mergeCDFs(
                cdfs
                  .asInstanceOf[Seq[(Buffer[Int32], BufferInt)]]
                  .map(v => (v._1.toSeq zip v._2.toSeq).toVector)
              )
              .unzip
            val xS = BufferInt(x.toArray).toSegment(
              outputPath.appendToTable(".locations")
            )
            val yS =
              BufferInt(y.toArray).toSegment(
              outputPath.appendToTable(".values")
            )
            IO.both(xS, yS).map(v => CDF(v._1, v._2 match {
              case x:SegmentInt =>x
              case _ => ???
            }))

        }
      }
  }

  def queue(inputs: Seq[(Segment[_], SegmentInt)], outputPath: LogicalPath)(
      implicit tsc: TaskSystemComponents
  ) =
    task(MergeCDFs(inputs, outputPath))(
      ResourceRequest(cpu = (1, 1), memory = 1, scratch = 0, gpu = 0)
    )
  implicit val codec: JsonValueCodec[MergeCDFs] = JsonCodecMaker.make
  implicit val code2: JsonValueCodec[CDF] =
    JsonCodecMaker.make
  val task = Task[MergeCDFs, CDF]("mergecdf", 1) { case input =>
    implicit ce =>
     doit(input.inputs,input.outputPath)

  }
}
