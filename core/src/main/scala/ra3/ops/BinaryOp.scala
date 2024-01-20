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

private[ra3] sealed trait BinaryOpDDD extends BinaryOp {
  type BufferTypeA = BufferDouble
  type SegmentTypeA = SegmentDouble
  type BufferTypeB = BufferDouble
  type SegmentTypeB = SegmentDouble
  type BufferTypeC = BufferDouble
  type SegmentTypeC = SegmentDouble
}

private[ra3] sealed trait BinaryOpDDI extends BinaryOp {
  type BufferTypeA = BufferDouble
  type SegmentTypeA = SegmentDouble
  type BufferTypeB = BufferDouble
  type SegmentTypeB = SegmentDouble
  type BufferTypeC = BufferInt
  type SegmentTypeC = SegmentInt
}

private[ra3] case class Op_ddd_multiply(a: SegmentDouble, b: SegmentDouble)
    extends BinaryOpDDD {
  def op(a: BufferDouble, b: BufferDouble): BufferDouble = {
    a.elementwise_*=(b)
    a
  }
}
private[ra3] case class Op_ddd_add(a: SegmentDouble, b: SegmentDouble)
    extends BinaryOpDDD {
  def op(a: BufferDouble, b: BufferDouble): BufferDouble = {
    a.elementwise_+=(b)
    a
  }
}
private[ra3] case class Op_ddi_eq(a: SegmentDouble, b: SegmentDouble)
    extends BinaryOpDDI {
  def op(a: BufferDouble, b: BufferDouble): BufferInt = {
    a.elementwise_eq(b)    
  }
}
private[ra3] case class Op_ddi_neq(a: SegmentDouble, b: SegmentDouble)
    extends BinaryOpDDI {
  def op(a: BufferDouble, b: BufferDouble): BufferInt = {
    a.elementwise_neq(b)    
  }
}
private[ra3] case class Op_ddi_gt(a: SegmentDouble, b: SegmentDouble)
    extends BinaryOpDDI {
  def op(a: BufferDouble, b: BufferDouble): BufferInt = {
    a.elementwise_gt(b)    
  }
}
private[ra3] case class Op_ddi_gteq(a: SegmentDouble, b: SegmentDouble)
    extends BinaryOpDDI {
  def op(a: BufferDouble, b: BufferDouble): BufferInt = {
    a.elementwise_gteq(b)    
  }
}
private[ra3] case class Op_ddi_lt(a: SegmentDouble, b: SegmentDouble)
    extends BinaryOpDDI {
  def op(a: BufferDouble, b: BufferDouble): BufferInt = {
    a.elementwise_lt(b)    
  }
}
private[ra3] case class Op_ddi_lteq(a: SegmentDouble, b: SegmentDouble)
    extends BinaryOpDDI {
  def op(a: BufferDouble, b: BufferDouble): BufferInt = {
    a.elementwise_lteq(b)    
  }
}