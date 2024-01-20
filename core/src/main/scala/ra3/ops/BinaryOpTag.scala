package ra3.ops
import ra3._


private[ra3]  sealed trait BinaryOpTag { self =>
  override def toString = name
  def name : String
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
private[ra3]  sealed trait BinaryOpTagDDD extends BinaryOpTag {
  type SegmentTypeA = SegmentDouble
  type SegmentTypeB = SegmentDouble
  type SegmentTypeC = SegmentDouble
  type ColumnTypeC = Column.F64Column
  type ColumnTagC = ColumnTag.F64.type
  val tagC = ColumnTag.F64
}
private[ra3]  sealed trait BinaryOpTagDDI extends BinaryOpTag {
  type SegmentTypeA = SegmentDouble
  type SegmentTypeB = SegmentDouble
  type SegmentTypeC = SegmentInt
  type ColumnTypeC = Column.Int32Column
  type ColumnTagC = ColumnTag.I32.type
  val tagC = ColumnTag.I32
}

private[ra3]  object BinaryOpTag {
  val dd_multiply = new BinaryOpTagDDD {

     def name = "op_ddd_multiple"

    type BinaryOpType = Op_ddd_multiply
    def op(a: SegmentTypeA, b: SegmentTypeB) = Op_ddd_multiply(a, b)

  }
  val dd_add = new BinaryOpTagDDD {

     def name = "op_ddd_add"

    type BinaryOpType = Op_ddd_add
    def op(a: SegmentTypeA, b: SegmentTypeB) = Op_ddd_add(a, b)

  }
  val dd_eq = new BinaryOpTagDDI{

     def name = "op_ddd_eq"

    type BinaryOpType = Op_ddi_eq
    def op(a: SegmentTypeA, b: SegmentTypeB) = Op_ddi_eq(a, b)

  }
  val dd_neq = new BinaryOpTagDDI{

     def name = "op_ddd_neq"

    type BinaryOpType = Op_ddi_neq
    def op(a: SegmentTypeA, b: SegmentTypeB) = Op_ddi_neq(a, b)

  }
  val dd_gt = new BinaryOpTagDDI{

     def name = "op_ddd_gt"

    type BinaryOpType = Op_ddi_gt
    def op(a: SegmentTypeA, b: SegmentTypeB) = Op_ddi_gt(a, b)

  }
  val dd_gteq = new BinaryOpTagDDI{

     def name = "op_ddd_gteq"

    type BinaryOpType = Op_ddi_gteq
    def op(a: SegmentTypeA, b: SegmentTypeB) = Op_ddi_gteq(a, b)

  }
  val dd_lt = new BinaryOpTagDDI{

     def name = "op_ddd_lt"

    type BinaryOpType = Op_ddi_lt
    def op(a: SegmentTypeA, b: SegmentTypeB) = Op_ddi_lt(a, b)

  }
  val dd_lteq = new BinaryOpTagDDI{

     def name = "op_ddd_lteq"

    type BinaryOpType = Op_ddi_lteq
    def op(a: SegmentTypeA, b: SegmentTypeB) = Op_ddi_lteq(a, b)

  }

}