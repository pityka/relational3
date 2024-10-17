package ra3.ts

import ra3.*
import tasks.*
import tasks.jsonitersupport.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import cats.effect.IO

private[ra3] case class MergeCDFs(
    input1: TaggedSegments,
    input2: Seq[SegmentDouble],
    outputPath: LogicalPath
)
private[ra3] object MergeCDFs {

  private def doitUntyped(
      input: MergeCDFs
  )(implicit tsc: TaskSystemComponents) = {
    val tag = input.input1.tag
    val xs = input.input1.segments
    val ys = input.input2
    doit(tag)(
      xs zip ys,
      input.outputPath
    )(tsc, tag.ordering)
  }

  private def doit(tag: ColumnTag)(
      inputs: Seq[(tag.SegmentType, SegmentDouble)],
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents,
      ordering: Ordering[tag.Elem]
  ): IO[UntypedCDF] = {
    IO.parSequenceN(32)(inputs.map { case (x, y) =>
      IO.both(tag.buffer(x), y.buffer)
    }).flatMap { cdfs =>
      val (x, y) = CDF
        .mergeCDFs(
          cdfs
            .map(v => (v._1.toSeq zip v._2.toSeq).toVector)
        )(using ordering)
        .unzip
      val xS = tag.toSegment(
        tag
          .makeBufferFromSeq(x*),
        outputPath.appendToTable(".locations")
      )
      val yS =
        BufferDouble(y.toArray).toSegment(
          outputPath.appendToTable(".values")
        )
      IO.both(xS, yS)
        .map(v => UntypedCDF(v._1, v._2))

    }
  }

  def queue(tag: ColumnTag)(
      inputs: Seq[(tag.SegmentType, SegmentDouble)],
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ) =
  task(
    MergeCDFs(
      tag.makeTaggedSegments(inputs.map(_._1)),
      inputs.map(_._2),
      outputPath
    )
  )(
    ResourceRequest(
      cpu = (1, 1),
      memory = inputs.map(v => ra3.Utils.guessMemoryUsageInMB(v._1) * 2).sum,
      scratch = 0,
      gpu = 0
    )
  ).map(_.toTyped(tag))
  // $COVERAGE-OFF$
  implicit val codec: JsonValueCodec[MergeCDFs] = JsonCodecMaker.make
  implicit val code2: JsonValueCodec[UntypedCDF] =
  JsonCodecMaker.make
  // $COVERAGE-ON$
  val task = Task[MergeCDFs, UntypedCDF]("mergecdf", 1) { case input =>
    implicit ce => doitUntyped(input)

  }
}
