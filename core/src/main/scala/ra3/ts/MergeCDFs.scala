package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

case class MergeCDFs(
    inputs: Seq[(Segment, SegmentDouble)],
    outputPath: LogicalPath
)
object MergeCDFs {

  private def doitUntyped(
      input: MergeCDFs
  )(implicit tsc: TaskSystemComponents) = {
    val tag = input.inputs.head._1.tag
    val xs= input.inputs.map(_._1).map(_.as(tag))
    val ys = input.inputs.map(_._2)
    doit(tag)(
      xs zip ys,
      input.outputPath
    )(tsc,tag.ordering)
  }

  private def doit(
       tag: ColumnTag)(
      inputs: Seq[(tag.SegmentType, SegmentDouble)],
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents,
      ordering: Ordering[tag.Elem],
  ): IO[CDF] = {
    IO.parSequenceN(32)(inputs.map { case (x, y) =>
      IO.both(x.buffer, y.buffer)
    }).flatMap { cdfs =>
      val (x, y) = CDF
        .mergeCDFs(
          cdfs
            .map(v => (v._1.toSeq zip v._2.toSeq).toVector)
        )(ordering)
        .unzip
      val xS = tag.makeBufferFromSeq(x:_*)
        .toSegment(
          outputPath.appendToTable(".locations")
        )
      val yS =
        BufferDouble(y.toArray).toSegment(
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
      inputs: Seq[(Segment, SegmentDouble)],
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ) =
    task(MergeCDFs(inputs, outputPath))(
      ResourceRequest(cpu = (1, 1), memory = inputs.map(v => ra3.Utils.guessMemoryUsageInMB(v._1)*2).sum, scratch = 0, gpu = 0)
    )
  implicit val codec: JsonValueCodec[MergeCDFs] = JsonCodecMaker.make
  implicit val code2: JsonValueCodec[CDF] =
    JsonCodecMaker.make
  val task = Task[MergeCDFs, CDF]("mergecdf", 1) { case input =>
    implicit ce => doitUntyped(input)

  }
}
