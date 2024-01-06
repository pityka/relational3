package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

case class MergeCDFs(
    inputs: Seq[(Segment[_ <: DataType], SegmentInt)],
    outputPath: LogicalPath
)
object MergeCDFs {

  private def doitUntyped(
      input: MergeCDFs
  )(implicit tsc: TaskSystemComponents) = {
    val dt = input.inputs.head._1.dType
    val ord = dt.ordering
    doit(dt)(
      input.inputs.map(pair => (pair._1.as[dt.type], pair._2)),
      input.outputPath
    )(tsc, ord)
  }

  private def doit[D <: DataType](dataType: D)(
      inputs: Seq[(dataType.SegmentType, SegmentInt)],
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents,
      ordering: Ordering[dataType.Elem]
  ): IO[CDF] = {
    IO.parSequenceN(32)(inputs.map { case (x, y) =>
      IO.both(x.buffer, y.buffer)
    }).flatMap { cdfs =>
      val (x, y) = Utils
        .mergeCDFs(
          cdfs
            .map(v => (v._1.toSeq zip v._2.toSeq).toVector)
        )
        .unzip
      val xS = dataType
        .bufferFromSeq(x: _*)
        .toSegment(
          outputPath.appendToTable(".locations")
        )
      val yS =
        BufferInt(y.toArray).toSegment(
          outputPath.appendToTable(".values")
        )
      IO.both(xS, yS)
        .map(v =>
          CDF(
            v._1,
            v._2
          )
        )

    }
  }

  def queue(
      inputs: Seq[(Segment[_ <: DataType], SegmentInt)],
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ) =
    task(MergeCDFs(inputs, outputPath))(
      ResourceRequest(cpu = (1, 1), memory = 1, scratch = 0, gpu = 0)
    )
  implicit val codec: JsonValueCodec[MergeCDFs] = JsonCodecMaker.make
  implicit val code2: JsonValueCodec[CDF] =
    JsonCodecMaker.make
  val task = Task[MergeCDFs, CDF]("mergecdf", 1) { case input =>
    implicit ce => doitUntyped(input)

  }
}
