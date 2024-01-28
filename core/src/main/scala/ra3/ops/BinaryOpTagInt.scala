package ra3.ops

import ra3._

private[ra3]  sealed trait BinaryOpTagIII extends BinaryOpTag {
  type SegmentTypeA = SegmentInt
  type SegmentTypeB = SegmentInt
  type ElemB = Int
  type SegmentTypeC = SegmentInt
  type ColumnTypeC = Column.Int32Column
  type ColumnTagC = ColumnTag.I32.type
  val tagC = ColumnTag.I32
}
private[ra3]  sealed trait BinaryOpTagIDD extends BinaryOpTag {
  type SegmentTypeA = SegmentInt
  type SegmentTypeB = SegmentDouble
  type ElemB = Double
  type SegmentTypeC = SegmentDouble
  type ColumnTypeC = Column.F64Column
  type ColumnTagC = ColumnTag.F64.type
  val tagC = ColumnTag.F64
}
private[ra3]  sealed trait BinaryOpTagIDI extends BinaryOpTag {
  type SegmentTypeA = SegmentInt
  type SegmentTypeB = SegmentDouble
  type ElemB = Double
  type SegmentTypeC = SegmentInt
  type ColumnTypeC = Column.Int32Column
  type ColumnTagC = ColumnTag.I32.type
  val tagC = ColumnTag.I32
}

private[ra3] object BinaryOpTagInt {
  object ii_and extends BinaryOpTagIII {

    type BinaryOpType = Op_iii_and
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) =
      Op_iii_and(a, b)

  }
  object ii_or extends BinaryOpTagIII {

    type BinaryOpType = Op_iii_or
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) =
      Op_iii_or(a, b)

  }
  object ii_multiply extends BinaryOpTagIII {

    type BinaryOpType = Op_iii_multiply
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) =
      Op_iii_multiply(a, b)

  }

  object id_multiply extends BinaryOpTagIDD {

    type BinaryOpType = Op_idd_multiply
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) =
      Op_idd_multiply(a, b)

  }
  object ii_add extends BinaryOpTagIII {

    type BinaryOpType = Op_iii_add
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_iii_add(a, b)

  }
  object id_add extends BinaryOpTagIDD {

    type BinaryOpType = Op_idd_add
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_idd_add(a, b)

  }
  object ii_eq extends BinaryOpTagIII {

    type BinaryOpType = Op_iii_eq
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_iii_eq(a, b)

  }
  object id_eq extends BinaryOpTagIDI {

    type BinaryOpType = Op_idi_eq
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_idi_eq(a, b)

  }
  object ii_neq extends BinaryOpTagIII {

    type BinaryOpType = Op_iii_neq
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_iii_neq(a, b)

  }
  object id_neq extends BinaryOpTagIDI {

    type BinaryOpType = Op_idi_neq
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_idi_neq(a, b)

  }
  object ii_gt extends BinaryOpTagIII {

    type BinaryOpType = Op_iii_gt
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_iii_gt(a, b)

  }
  object id_gt extends BinaryOpTagIDI {

    type BinaryOpType = Op_idi_gt
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_idi_gt(a, b)

  }
  object ii_gteq extends BinaryOpTagIII {

    type BinaryOpType = Op_iii_gteq
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_iii_gteq(a, b)

  }
  object id_gteq extends BinaryOpTagIDI {

    type BinaryOpType = Op_idi_gteq
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_idi_gteq(a, b)

  }
  object ii_lt extends BinaryOpTagIII {

    type BinaryOpType = Op_iii_lt
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_iii_lt(a, b)

  }
  object id_lt extends BinaryOpTagIDI {

    type BinaryOpType = Op_idi_lt
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_idi_lt(a, b)

  }
  object ii_lteq extends BinaryOpTagIII {

    type BinaryOpType = Op_iii_lteq
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_iii_lteq(a, b)

  }
  object id_lteq extends BinaryOpTagIDI {

    type BinaryOpType = Op_idi_lteq
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_idi_lteq(a, b)

  }
}