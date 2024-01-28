package ra3.ops
import ra3._

private[ra3] sealed trait BinaryOpTagDDD extends BinaryOpTag {
  type SegmentTypeA = SegmentDouble
  type SegmentTypeB = SegmentDouble
  type ElemB = Double
  type SegmentTypeC = SegmentDouble
  type ColumnTypeC = Column.F64Column
  type ColumnTagC = ColumnTag.F64.type
  val tagC = ColumnTag.F64
}
private[ra3] sealed trait BinaryOpTagDLD extends BinaryOpTag {
  type SegmentTypeA = SegmentDouble
  type SegmentTypeB = SegmentLong
  type ElemB = Long
  type SegmentTypeC = SegmentDouble
  type ColumnTypeC = Column.F64Column
  type ColumnTagC = ColumnTag.F64.type
  val tagC = ColumnTag.F64
}
private[ra3] sealed trait BinaryOpTagDDI extends BinaryOpTag {
  type SegmentTypeA = SegmentDouble
  type SegmentTypeB = SegmentDouble
  type ElemB = Double
  type SegmentTypeC = SegmentInt
  type ColumnTypeC = Column.Int32Column
  type ColumnTagC = ColumnTag.I32.type
  val tagC = ColumnTag.I32
}
private[ra3] sealed trait BinaryOpTagDLI extends BinaryOpTag {
  type SegmentTypeA = SegmentDouble
  type SegmentTypeB = SegmentLong
  type ElemB = Long
  type SegmentTypeC = SegmentInt
  type ColumnTypeC = Column.Int32Column
  type ColumnTagC = ColumnTag.I32.type
  val tagC = ColumnTag.I32
}

private[ra3] object BinaryOpTagDouble {
  object dd_multiply extends BinaryOpTagDDD {

    type BinaryOpType = Op_ddd_multiply
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) =
      Op_ddd_multiply(a, b)

  }
  object dl_multiply extends BinaryOpTagDLD {

    type BinaryOpType = Op_dld_multiply
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) =
      Op_dld_multiply(a, b)

  }
  object dd_add extends BinaryOpTagDDD {

    type BinaryOpType = Op_ddd_add
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_ddd_add(a, b)

  }
  object dl_add extends BinaryOpTagDLD {

    type BinaryOpType = Op_dld_add
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_dld_add(a, b)

  }
  object dd_eq extends BinaryOpTagDDI {

    type BinaryOpType = Op_ddi_eq
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_ddi_eq(a, b)

  }
  object dl_eq extends BinaryOpTagDLI {

    type BinaryOpType = Op_dli_eq
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_dli_eq(a, b)

  }
  object dd_neq extends BinaryOpTagDDI {

    type BinaryOpType = Op_ddi_neq
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_ddi_neq(a, b)

  }
  object dl_neq extends BinaryOpTagDLI {

    type BinaryOpType = Op_dli_neq
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_dli_neq(a, b)

  }
  object dd_gt extends BinaryOpTagDDI {

    type BinaryOpType = Op_ddi_gt
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_ddi_gt(a, b)

  }
  object dl_gt extends BinaryOpTagDLI {

    type BinaryOpType = Op_dli_gt
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_dli_gt(a, b)

  }
  object dd_gteq extends BinaryOpTagDDI {

    type BinaryOpType = Op_ddi_gteq
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_ddi_gteq(a, b)

  }
  object dl_gteq extends BinaryOpTagDLI {

    type BinaryOpType = Op_dli_gteq
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_dli_gteq(a, b)

  }
  object dd_lt extends BinaryOpTagDDI {

    type BinaryOpType = Op_ddi_lt
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_ddi_lt(a, b)

  }
  object dl_lt extends BinaryOpTagDLI {

    type BinaryOpType = Op_dli_lt
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_dli_lt(a, b)

  }
  object dd_lteq extends BinaryOpTagDDI {

    type BinaryOpType = Op_ddi_lteq
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_ddi_lteq(a, b)

  }
  object dl_lteq extends BinaryOpTagDLI {

    type BinaryOpType = Op_dli_lteq
    def op(a: SegmentTypeA, b: Either[SegmentTypeB, ElemB]) = Op_dli_lteq(a, b)

  }

}
