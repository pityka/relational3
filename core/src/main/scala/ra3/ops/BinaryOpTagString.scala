package ra3.ops

import ra3._

private[ra3]  sealed trait BinaryOpTagSSI extends BinaryOpTag {
  type SegmentTypeA = SegmentString
  type SegmentTypeB = SegmentString
  type ElemB = CharSequence
  type SegmentTypeC = SegmentInt
  type ColumnTypeC = Column.Int32Column
  type ColumnTagC = ColumnTag.I32.type
  val tagC = ColumnTag.I32
}

private[ra3] object BinaryOpTagString {
  
  object ss_eq extends BinaryOpTagSSI {

    type BinaryOpType = Op_ssi_eq
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_ssi_eq(a, b)

  }
 
  object ss_neq extends BinaryOpTagSSI {

    type BinaryOpType = Op_ssi_neq
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_ssi_neq(a, b)

  }
  object ss_gt extends BinaryOpTagSSI {

    type BinaryOpType = Op_ssi_gt
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_ssi_gt(a, b)

  }
 
  object ss_gteq extends BinaryOpTagSSI {

    type BinaryOpType = Op_ssi_gteq
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_ssi_gteq(a, b)

  }
 
  object ss_lt extends BinaryOpTagSSI {

    type BinaryOpType = Op_ssi_lt
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_ssi_lt(a, b)

  }

  object ss_lteq extends BinaryOpTagSSI {

    type BinaryOpType = Op_ssi_lteq
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_ssi_lteq(a, b)

  }
 
}