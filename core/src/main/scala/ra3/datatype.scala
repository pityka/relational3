package ra3

sealed trait DataType { self : DataType =>
  type Elem 
  type SegmentType <: Segment {
    type DType = self.type
  }
  type BufferType <: Buffer{
    type DType = self.type
  }
  type ColumnType <: Column{
    type DType = self.type
  }
  def cast(s: Segment) = s.asInstanceOf[SegmentType]
  def pair(a: SegmentType, b: SegmentType) : SegmentPair{
    type DType = self.type
  }

  def makeColumn(s: Vector[SegmentType]): ColumnType
  def bufferFromSeq(es: Elem*) : BufferType
  def ordering: Ordering[Elem]


  


}

case object Int32 extends DataType {
  type Elem = Int
  type BufferType = BufferInt
  type SegmentType = SegmentInt
  type ColumnType = Column.Int32Column
    def makeColumn(s: Vector[SegmentType]): ColumnType = Column.Int32Column(s)
def pair(a: SegmentType, b: SegmentType)  = I32Pair(a,b)
def bufferFromSeq(es: Elem*) : BufferType = BufferInt(es.toArray)
def ordering : Ordering[Int] = implicitly[Ordering[Int]]

}
case object F64 extends DataType {
  type Elem = Double
  type BufferType = BufferDouble
  type SegmentType = SegmentDouble
  type ColumnType = Column.F64Column
  def makeColumn(s: Vector[SegmentType]): ColumnType = Column.F64Column(s)
  def pair(a: SegmentType, b: SegmentType)  = F64Pair(a,b)

  def bufferFromSeq(es: Elem*) : BufferType = ???
  def ordering : Ordering[Double] = implicitly[Ordering[Double]]
}

trait DataTypes {
  type Int32 = Int32.type
  type F64 = F64.type

}
