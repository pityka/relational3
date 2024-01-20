package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

sealed trait BinaryOpTag { self =>
  type SegmentTypeA <: Segment {
    type SegmentType = self.SegmentTypeA
  }

  type SegmentTypeB <: Segment {
    type SegmentType = self.SegmentTypeB
  }
  type ColumnTypeC <: Column {
    type ColumnType = self.ColumnTypeC
    type SegmentType = self.SegmentTypeC
  }
  type ColumnTagC <: ColumnTag {
    type ColumnType = self.ColumnTypeC
    type SegmentType = self.SegmentTypeC
  }
  type SegmentTypeC <: Segment {
    type SegmentType = self.SegmentTypeC
    type ColumnType = self.ColumnTypeC
  }
  type BinaryOpType <: BinaryOp {
    type SegmentTypeA = self.SegmentTypeA
    type SegmentTypeB = self.SegmentTypeB
    type SegmentTypeC = self.SegmentTypeC
  }
  def op(a: SegmentTypeA, b: SegmentTypeB): BinaryOpType
  def tagC: ColumnTagC
}
sealed trait BinaryOpTagDDD extends BinaryOpTag {
  type SegmentTypeA = SegmentDouble
  type SegmentTypeB = SegmentDouble
  type SegmentTypeC = SegmentDouble
  type ColumnTypeC = Column.F64Column
  type ColumnTagC = ColumnTag.F64.type
  val tagC = ColumnTag.F64
}
object BinaryOpTag {
  val ddd_* = new BinaryOpTagDDD {

    type BinaryOpType = BinaryOpDDD_*
    def op(a: SegmentTypeA, b: SegmentTypeB) = BinaryOpDDD_*(a, b)

  }
}

/* op owns the inputs */
sealed trait BinaryOp { self =>
  type BinaryOpType >: this.type <: BinaryOp
  type BufferTypeA <: Buffer {
    type SegmentType = self.SegmentTypeA
    type BufferType = self.BufferTypeA
  }
  type SegmentTypeA <: Segment {
    type BufferType = self.BufferTypeA
    type SegmentType = self.SegmentTypeA
  }
  type BufferTypeB <: Buffer {
    type SegmentType = self.SegmentTypeB
    type BufferType = self.BufferTypeB
  }
  type SegmentTypeB <: Segment {
    type BufferType = self.BufferTypeB
    type SegmentType = self.SegmentTypeB
  }
  type BufferTypeC <: Buffer {
    type SegmentType = self.SegmentTypeC
    type BufferType = self.BufferTypeC
  }
  type SegmentTypeC <: Segment {
    type BufferType = self.BufferTypeC
    type SegmentType = self.SegmentTypeC
  }
  def a: SegmentTypeA
  def b: SegmentTypeB
  def op(a: BufferTypeA, b: BufferTypeB): BufferTypeC
}

sealed trait BinaryOpDDD extends BinaryOp {
  type BufferTypeA = BufferDouble
  type SegmentTypeA = SegmentDouble
  type BufferTypeB = BufferDouble
  type SegmentTypeB = SegmentDouble
  type BufferTypeC = BufferDouble
  type SegmentTypeC = SegmentDouble
}

case class BinaryOpDDD_*(a: SegmentDouble, b: SegmentDouble)
    extends BinaryOpDDD {
  def op(a: BufferDouble, b: BufferDouble): BufferDouble = {
    a.elementwise_*=(b)
    a
  }
}

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
    val b = input.b.buffer
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
      ResourceRequest(cpu = (1, 1), memory = 1, scratch = 0, gpu = 0)
    ).map(_.as[inputs.SegmentTypeC])
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
