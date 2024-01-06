package ra3

sealed trait DataType { self =>
  type Elem 
  type SegmentType <: Segment[self.type]
  type BufferType <: Buffer[self.type]
  type ColumnType <: Column[self.type]
  def cast(s: Segment[_]) = s.asInstanceOf[SegmentType]
  def pair(a: SegmentType, b: SegmentType) : SegmentPair[self.type]

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
