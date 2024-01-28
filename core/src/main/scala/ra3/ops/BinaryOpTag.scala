package ra3.ops
import ra3._


private[ra3]  trait BinaryOpTag { self =>
  type SegmentTypeA <: Segment {
    type SegmentType = self.SegmentTypeA
  }

  type ElemB 

  type SegmentTypeB <: Segment {
    type SegmentType = self.SegmentTypeB
    type Elem = self.ElemB
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
  def op(a: SegmentTypeA, b: Either[SegmentTypeB,ElemB]): BinaryOpType
  def tagC: ColumnTagC
}
