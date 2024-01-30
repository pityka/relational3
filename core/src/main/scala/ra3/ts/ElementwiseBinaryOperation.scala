package ra3.ts

import ra3._
import ra3.ops.BinaryOp
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

case class ElementwiseBinaryOperation(
    inputs: BinaryOp,
    outputPath: LogicalPath
)
object ElementwiseBinaryOperation {
  def doit(
      input: BinaryOp,
      outputPath: LogicalPath
  )(implicit tsc: TaskSystemComponents): IO[input.SegmentTypeC] = {
    val a = input.a.buffer
    val b = input.b match {
      case Left(segment) => segment.buffer
      case Right(elem) =>
        IO.pure(input.tagB.broadcastBuffer(elem, input.a.numElems))
    }
    IO.both(a, b).flatMap { case (a, b) =>
      input.op(a, b).toSegment(outputPath)
    }
  }
  def queue(
      inputs: BinaryOp,
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ): IO[inputs.SegmentTypeC] = {

    task(ElementwiseBinaryOperation(inputs, outputPath))(
      ResourceRequest(cpu = (1, 1), memory = ra3.Utils.guessMemoryUsageInMB(inputs.a)*2, scratch = 0, gpu = 0)
    ).map(_.as[inputs.SegmentTypeC])
  }
  @scala.annotation.nowarn
  private implicit val codec0: JsonValueCodec[CharSequence] =
    new JsonValueCodec[CharSequence] {
      override def decodeValue(in: JsonReader, default: CharSequence): String =
        if (in.isNextToken('"')) {
            in.rollbackToken()
            in.readString(null)
          } else {
            in.decodeError("expected string")
          }

      override def encodeValue(
          x: CharSequence,
          out: JsonWriter
      ): Unit = out.writeVal(x.toString)

      override val nullValue: CharSequence = null
    }
  implicit val codec: JsonValueCodec[ElementwiseBinaryOperation] =
    JsonCodecMaker.make
  implicit val codecOut: JsonValueCodec[Segment] = JsonCodecMaker.make
  val task =
    Task[ElementwiseBinaryOperation, Segment]("ElementwiseBinaryOperation", 1) {
      case input =>
        implicit ce => doit(input.inputs, input.outputPath)

    }
}
