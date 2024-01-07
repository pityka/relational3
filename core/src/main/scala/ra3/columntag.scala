package ra3

sealed trait ColumnTag  { self =>
  type ColumnTagType >: this.type <: ColumnTag 
  type SegmentPairType <: SegmentPair {
    type SegmentType  = self.SegmentType
    type BufferType = self.BufferType
    type Elem = self.Elem
  }
  type SegmentType <: Segment {
    type ColumnTagType = self.ColumnTagType
    type SegmentType  = self.SegmentType
    type BufferType = self.BufferType
    type Elem = self.Elem
  }
  type BufferType <: Buffer {    
    type SegmentType  = self.SegmentType
    type BufferType = self.BufferType
    type Elem = self.Elem
  }
  type Elem
  type ColumnType <: Column {
    type ColumnTagType = self.ColumnTagType
    type SegmentType  = self.SegmentType
    type BufferType = self.BufferType
    type Elem = self.Elem
  }
  def cast(s:Vector[Segment]) : Vector[SegmentType] = s.asInstanceOf[Vector[SegmentType]]
  def makeBuffer(elems: Array[Elem]) : BufferType
  def makeBufferFromSeq(elems: Elem*) : BufferType //BufferDouble(elems)
  def makeColumn(segments: Vector[SegmentType]) : ColumnType
  def ordering: Ordering[Elem]
  def pair(a: SegmentType, b: SegmentType) : SegmentPairType
}
object ColumnTag {
object I32 extends ColumnTag {
  override def toString="I32"
  type Elem = Int
  type BufferType = BufferInt
  type ColumnTagType = I32.type 
  type ColumnType = Column.Int32Column
  type SegmentType = SegmentInt
  type SegmentPairType = I32Pair
  def makeBuffer(elems: Array[Int]) : BufferType = BufferInt(elems)
  def makeBufferFromSeq(elems: Elem*) : BufferType = BufferInt(elems.toArray)
  def makeColumn(segments: Vector[SegmentType]) : ColumnType = Column.Int32Column(segments)
  val ordering = implicitly[Ordering[Elem]]
  def pair(a: SegmentType, b: SegmentType) : SegmentPairType = I32Pair(a,b)
}
object F64 extends ColumnTag {
  override def toString="F64"
  type Elem = Double
  type BufferType = BufferDouble
  type ColumnTagType = F64.type 
  type ColumnType = Column.F64Column
  type SegmentType = SegmentDouble
  type SegmentPairType = F64Pair
  def makeBuffer(elems: Array[Double]) : BufferType = ??? //BufferDouble(elems)
  def makeBufferFromSeq(elems: Double*) : BufferType = ??? //BufferDouble(elems)
  def makeColumn(segments: Vector[SegmentType]) : ColumnType = ???
  val ordering = implicitly[Ordering[Elem]]
  def pair(a: SegmentType, b: SegmentType) : SegmentPairType = F64Pair(a,b)
}
}

trait ColumnTags {
  implicit val i32: ColumnTag.I32.type = ColumnTag.I32
  implicit val f64: ColumnTag.F64.type = ColumnTag.F64
}