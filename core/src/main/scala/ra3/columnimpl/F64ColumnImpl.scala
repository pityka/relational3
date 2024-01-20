package ra3.columnimpl
import ra3._
import tasks.TaskSystemComponents
import cats.effect.IO
trait F64ColumnImpl { self: Column.F64Column =>

  // def elementwise[OP<:ts.BinaryOpTag{ type SegmentTypeA = SegmentDouble ; type SegmentTypeB = SegmentDouble}](
  //     other: Column.F64Column,
  //     opTag: OP
  // )(implicit tsc: TaskSystemComponents): IO[opTag.ColumnTypeC] = {
  //   assert(self.segments.size == other.segments.size)
  //   IO.parSequenceN(math.min(32, self.segments.size))(
  //     self.segments.zip(other.segments).zipWithIndex.map {
  //       case ((a, b), segmentIdx) =>
  //         assert(a.numElems == b.numElems)
  //         ts.ElementwiseBinaryOperation
  //           .queue(opTag.op(a, b), LogicalPath(???, None, segmentIdx, 0))
  //     }
  //   ).map(segments => opTag.tagC.makeColumn(segments))
  // }

  def *(other: Column.F64Column)(implicit tsc: TaskSystemComponents) : IO[Column.F64Column] = 
      elementwise(other,ra3.ts.BinaryOpTag.ddd_*)

}
