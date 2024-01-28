package ra3.ops
import ra3._

/* op owns the inputs */
private[ra3] sealed trait BinaryOp { self =>
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
  type ElemB 
  type SegmentTypeB <: Segment {
    type Elem = self.ElemB
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
  def b: Either[SegmentTypeB,ElemB]
  type ColumnTagB <: ColumnTag {
    type BufferType = self.BufferTypeB
    type SegmentType = self.SegmentTypeB
    type Elem = self.ElemB
  }
  def tagB : ColumnTagB
  def op(a: BufferTypeA, b: BufferTypeB): BufferTypeC
}

private[ra3] sealed trait BinaryOpDDD extends BinaryOp {
  val tagB = ColumnTag.F64 
  type ColumnTagB = ColumnTag.F64.type 
  type BufferTypeA = BufferDouble
  type SegmentTypeA = SegmentDouble

  type ElemB = Double
  type BufferTypeB = BufferDouble
  type SegmentTypeB = SegmentDouble
  type BufferTypeC = BufferDouble
  type SegmentTypeC = SegmentDouble
}
private[ra3] sealed trait BinaryOpDLD extends BinaryOp {
  val tagB = ColumnTag.I64 
  type ColumnTagB = ColumnTag.I64.type 
  type BufferTypeA = BufferDouble
  type SegmentTypeA = SegmentDouble

  type ElemB = Long
  type BufferTypeB = BufferLong
  type SegmentTypeB = SegmentLong
  type BufferTypeC = BufferDouble
  type SegmentTypeC = SegmentDouble
}

private[ra3] sealed trait BinaryOpDDI extends BinaryOp {

  val tagB = ColumnTag.F64 
  type ColumnTagB = ColumnTag.F64.type 
  type BufferTypeA = BufferDouble
  type SegmentTypeA = SegmentDouble
  type BufferTypeB = BufferDouble
  type SegmentTypeB = SegmentDouble
  type ElemB = Double
  type BufferTypeC = BufferInt
  type SegmentTypeC = SegmentInt
}
private[ra3] sealed trait BinaryOpDLI extends BinaryOp {

  val tagB = ColumnTag.I64 
  type ColumnTagB = ColumnTag.I64.type 
  type BufferTypeA = BufferDouble
  type SegmentTypeA = SegmentDouble
  type BufferTypeB = BufferLong
  type SegmentTypeB = SegmentLong
  type ElemB = Long
  type BufferTypeC = BufferInt
  type SegmentTypeC = SegmentInt
}
private[ra3] sealed trait BinaryOpIII extends BinaryOp {

  val tagB = ColumnTag.I32
  type ColumnTagB = ColumnTag.I32.type 
  type BufferTypeA = BufferInt
  type SegmentTypeA = SegmentInt
  type BufferTypeB = BufferInt
  type SegmentTypeB = SegmentInt
  type ElemB = Int
  type BufferTypeC = BufferInt
  type SegmentTypeC = SegmentInt
}
private[ra3] sealed trait BinaryOpIDD extends BinaryOp {

  val tagB = ColumnTag.F64
  type ColumnTagB = ColumnTag.F64.type 
  type BufferTypeA = BufferInt
  type SegmentTypeA = SegmentInt
  type BufferTypeB = BufferDouble
  type SegmentTypeB = SegmentDouble
  type ElemB = Double
  type BufferTypeC = BufferDouble
  type SegmentTypeC = SegmentDouble
}
private[ra3] sealed trait BinaryOpIDI extends BinaryOp {

  val tagB = ColumnTag.F64
  type ColumnTagB = ColumnTag.F64.type 
  type BufferTypeA = BufferInt
  type SegmentTypeA = SegmentInt
  type BufferTypeB = BufferDouble
  type SegmentTypeB = SegmentDouble
  type ElemB = Double
  type BufferTypeC = BufferInt
  type SegmentTypeC = SegmentInt
}

private[ra3] sealed trait BinaryOpSSI extends BinaryOp {

  val tagB = ColumnTag.StringTag
  type ColumnTagB = ColumnTag.StringTag.type 
  type BufferTypeA = BufferString
  type SegmentTypeA = SegmentString
  type BufferTypeB = BufferString
  type SegmentTypeB = SegmentString
  type ElemB = CharSequence
  type BufferTypeC = BufferInt
  type SegmentTypeC = SegmentInt
}

private[ra3] case class Op_ddd_multiply(a: SegmentDouble, b: Either[SegmentDouble,Double])
    extends BinaryOpDDD {
  def op(a: BufferDouble, b: BufferDouble): BufferDouble = {
    a.elementwise_*=(b)
    a
  }
}
private[ra3] case class Op_dld_multiply(a: SegmentDouble, b: Either[SegmentLong,Long])
    extends BinaryOpDLD {
  def op(a: BufferDouble, b: BufferLong ): BufferDouble = {
    a.elementwise_*=(b)
    a
  }
}
private[ra3] case class Op_ddd_add(a: SegmentDouble, b: Either[SegmentDouble,Double])
    extends BinaryOpDDD {
  def op(a: BufferDouble, b: BufferDouble): BufferDouble = {
    a.elementwise_+=(b)
    a
  }
}
private[ra3] case class Op_dld_add(a: SegmentDouble, b: Either[SegmentLong,Long])
    extends BinaryOpDLD {
  def op(a: BufferDouble, b: BufferLong): BufferDouble = {
    a.elementwise_+=(b)
    a
  }
}
private[ra3] case class Op_ddi_eq(a: SegmentDouble, b: Either[SegmentDouble,Double])
    extends BinaryOpDDI {
  def op(a: BufferDouble, b: BufferDouble): BufferInt = {
    a.elementwise_eq(b)    
  }
}
private[ra3] case class Op_ddi_neq(a: SegmentDouble, b: Either[SegmentDouble,Double])
    extends BinaryOpDDI {
  def op(a: BufferDouble, b: BufferDouble): BufferInt = {
    a.elementwise_neq(b)    
  }
}
private[ra3] case class Op_ddi_gt(a: SegmentDouble, b: Either[SegmentDouble,Double])
    extends BinaryOpDDI {
  def op(a: BufferDouble, b: BufferDouble): BufferInt = {
    a.elementwise_gt(b)    
  }
}
private[ra3] case class Op_ddi_gteq(a: SegmentDouble, b: Either[SegmentDouble,Double])
    extends BinaryOpDDI {
  def op(a: BufferDouble, b: BufferDouble): BufferInt = {
    a.elementwise_gteq(b)    
  }
}
private[ra3] case class Op_ddi_lt(a: SegmentDouble, b: Either[SegmentDouble,Double])
    extends BinaryOpDDI {
  def op(a: BufferDouble, b: BufferDouble): BufferInt = {
    a.elementwise_lt(b)    
  }
}
private[ra3] case class Op_ddi_lteq(a: SegmentDouble, b: Either[SegmentDouble,Double])
    extends BinaryOpDDI {
  def op(a: BufferDouble, b: BufferDouble): BufferInt = {
    a.elementwise_lteq(b)    
  }
}
// DLI

private[ra3] case class Op_dli_eq(a: SegmentDouble, b: Either[SegmentLong,Long])
    extends BinaryOpDLI {
  def op(a: BufferDouble, b: BufferLong): BufferInt = {
    a.elementwise_eq(b)    
  }
}
private[ra3] case class Op_dli_neq(a: SegmentDouble, b: Either[SegmentLong,Long])
    extends BinaryOpDLI {
  def op(a: BufferDouble, b: BufferLong): BufferInt = {
    a.elementwise_neq(b)    
  }
}
private[ra3] case class Op_dli_gt(a: SegmentDouble, b: Either[SegmentLong,Long])
    extends BinaryOpDLI {
  def op(a: BufferDouble, b: BufferLong): BufferInt = {
    a.elementwise_gt(b)    
  }
}
private[ra3] case class Op_dli_gteq(a: SegmentDouble, b: Either[SegmentLong,Long])
    extends BinaryOpDLI {
  def op(a: BufferDouble, b: BufferLong): BufferInt = {
    a.elementwise_gteq(b)    
  }
}
private[ra3] case class Op_dli_lt(a: SegmentDouble, b: Either[SegmentLong,Long])
    extends BinaryOpDLI {
  def op(a: BufferDouble, b: BufferLong): BufferInt = {
    a.elementwise_lt(b)    
  }
}
private[ra3] case class Op_dli_lteq(a: SegmentDouble, b: Either[SegmentLong,Long])
    extends BinaryOpDLI {
  def op(a: BufferDouble, b: BufferLong): BufferInt = {
    a.elementwise_lteq(b)    
  }
}

// III 
private[ra3] case class Op_iii_and(a: SegmentInt, b: Either[SegmentInt,Int])
    extends BinaryOpIII {
  def op(a: BufferInt, b: BufferInt): BufferInt = {
    a.elementwise_&&(b)
    
  }
}
private[ra3] case class Op_iii_or(a: SegmentInt, b: Either[SegmentInt,Int])
    extends BinaryOpIII {
  def op(a: BufferInt, b: BufferInt): BufferInt = {
    a.elementwise_||(b)
    
  }
}
private[ra3] case class Op_iii_multiply(a: SegmentInt, b: Either[SegmentInt,Int])
    extends BinaryOpIII {
  def op(a: BufferInt, b: BufferInt): BufferInt = {
    a.elementwise_*=(b)
    a
  }
}
private[ra3] case class Op_iii_add(a: SegmentInt, b: Either[SegmentInt,Int])
    extends BinaryOpIII {
  def op(a: BufferInt, b: BufferInt): BufferInt = {
    a.elementwise_+=(b)
    a
  }
}
private[ra3] case class Op_iii_eq(a: SegmentInt, b: Either[SegmentInt,Int])
    extends BinaryOpIII {
  def op(a: BufferInt, b: BufferInt): BufferInt = {
    a.elementwise_eq(b)
    a
  }
}
private[ra3] case class Op_iii_neq(a: SegmentInt, b: Either[SegmentInt,Int])
    extends BinaryOpIII {
  def op(a: BufferInt, b: BufferInt): BufferInt = {
    a.elementwise_neq(b)
    a
  }
}
private[ra3] case class Op_iii_lt(a: SegmentInt, b: Either[SegmentInt,Int])
    extends BinaryOpIII {
  def op(a: BufferInt, b: BufferInt): BufferInt = {
    a.elementwise_lt(b)
    a
  }
}
private[ra3] case class Op_iii_lteq(a: SegmentInt, b: Either[SegmentInt,Int])
    extends BinaryOpIII {
  def op(a: BufferInt, b: BufferInt): BufferInt = {
    a.elementwise_lteq(b)
    a
  }
}
private[ra3] case class Op_iii_gt(a: SegmentInt, b: Either[SegmentInt,Int])
    extends BinaryOpIII {
  def op(a: BufferInt, b: BufferInt): BufferInt = {
    a.elementwise_gt(b)
    a
  }
}
private[ra3] case class Op_iii_gteq(a: SegmentInt, b: Either[SegmentInt,Int])
    extends BinaryOpIII {
  def op(a: BufferInt, b: BufferInt): BufferInt = {
    a.elementwise_gteq(b)
    a
  }
}

// IDD

private[ra3] case class Op_idd_multiply(a: SegmentInt, b: Either[SegmentDouble,Double])
    extends BinaryOpIDD {
  def op(a: BufferInt, b: BufferDouble): BufferDouble = {
    a.elementwise_*(b)
    
  }
}
private[ra3] case class Op_idd_add(a: SegmentInt, b: Either[SegmentDouble,Double])
    extends BinaryOpIDD {
  def op(a: BufferInt, b: BufferDouble): BufferDouble = {
    a.elementwise_+(b)
    
  }
}

// IDI

private[ra3] case class Op_idi_eq(a: SegmentInt, b: Either[SegmentDouble,Double])
    extends BinaryOpIDI {
  def op(a: BufferInt, b: BufferDouble): BufferInt = {
    a.elementwise_eq(b)
    a
  }
}
private[ra3] case class Op_idi_neq(a: SegmentInt, b: Either[SegmentDouble,Double])
    extends BinaryOpIDI {
  def op(a: BufferInt, b: BufferDouble): BufferInt = {
    a.elementwise_neq(b)
    a
  }
}
private[ra3] case class Op_idi_lt(a: SegmentInt, b:Either[SegmentDouble,Double])
    extends BinaryOpIDI {
  def op(a: BufferInt, b: BufferDouble): BufferInt = {
    a.elementwise_lt(b)
    a
  }
}
private[ra3] case class Op_idi_lteq(a: SegmentInt, b: Either[SegmentDouble,Double])
    extends BinaryOpIDI {
  def op(a: BufferInt, b: BufferDouble): BufferInt = {
    a.elementwise_lteq(b)
    a
  }
}
private[ra3] case class Op_idi_gt(a: SegmentInt, b: Either[SegmentDouble,Double])
    extends BinaryOpIDI {
  def op(a: BufferInt, b: BufferDouble): BufferInt = {
    a.elementwise_gt(b)
    a
  }
}
private[ra3] case class Op_idi_gteq(a: SegmentInt, b: Either[SegmentDouble,Double])
    extends BinaryOpIDI {
  def op(a: BufferInt, b: BufferDouble): BufferInt = {
    a.elementwise_gteq(b)
    a
  }
}

// SSI

private[ra3] case class Op_ssi_eq(a: SegmentString, b: Either[SegmentString,CharSequence])
    extends BinaryOpSSI {
  def op(a: BufferString, b: BufferString): BufferInt = {
    a.elementwise_eq(b)    
  }
}
private[ra3] case class Op_ssi_neq(a: SegmentString, b: Either[SegmentString,CharSequence])
    extends BinaryOpSSI {
  def op(a: BufferString, b: BufferString): BufferInt = {
    a.elementwise_neq(b)    
  }
}
private[ra3] case class Op_ssi_gt(a: SegmentString, b: Either[SegmentString,CharSequence])
    extends BinaryOpSSI {
  def op(a: BufferString, b: BufferString): BufferInt = {
    a.elementwise_gt(b)    
  }
}
private[ra3] case class Op_ssi_gteq(a: SegmentString, b: Either[SegmentString,CharSequence])
    extends BinaryOpSSI {
  def op(a: BufferString, b: BufferString): BufferInt = {
    a.elementwise_gteq(b)    
  }
}
private[ra3] case class Op_ssi_lt(a: SegmentString, b: Either[SegmentString,CharSequence])
    extends BinaryOpSSI {
  def op(a: BufferString, b: BufferString): BufferInt = {
    a.elementwise_lt(b)    
  }
}
private[ra3] case class Op_ssi_lteq(a: SegmentString, b: Either[SegmentString,CharSequence])
    extends BinaryOpSSI {
  def op(a: BufferString, b: BufferString): BufferInt = {
    a.elementwise_lteq(b)    
  }
}