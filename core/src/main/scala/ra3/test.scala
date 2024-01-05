package test
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
sealed trait DataType { self: DataType =>
  type Elem
  type SegmentType <: Segment[self.type]
  type BufferType <: Buffer[self.type]
  type ColumnType <: Column[self.type]
  def cast(s: Segment[_]) = s.asInstanceOf[SegmentType]
  def makeColumn(s: Vector[SegmentType]): ColumnType
  // def cast(s:Buffer[_]) : BufferType

}
sealed trait Statistic[T]
sealed trait Buffer[DType <: DataType] { // self: DType#BufferType =>
  // type SegType = DType#SegmentType

  def toSeq: Seq[DType#Elem]
  def toSegment: DType#SegmentType
}
sealed trait Segment[DType <: DataType] {  self =>
  // type BufferType = DType#BufferType
  type Elem = DType#Elem
  type SegmentType = DType#SegmentType
  def buffer: Buffer[DType]
  def statistic: Statistic[Elem]
  // def pair(other: DType#SegmentType) : Int
  // def cast[D<:DataType] = this.asInstanceOf[Segment[D]]
}
sealed trait Column[DType <: DataType] { self =>
  // type SegType = DType#SegmentType
  val dataType: DType
  def segments: Vector[dataType.SegmentType]
  // val z: Seq[Elem]
  // val x = segments.head:Segment[DType]

  def ++(other: self.type) = {
    val both : Vector[dataType.SegmentType]= segments ++ other.segments
    Column(dataType)(both)
  }
}

case object Int32 extends DataType {

  // override def cast(s: Segment[_]): SegmentType = s.asInstanceOf[SegmentInt]

  type Elem = Int
  type BufferType = BufferInt
  type SegmentType = SegmentInt
  type ColumnType = Int32Column
  def makeColumn(s: Vector[SegmentType]): ColumnType = Int32Column(s)

}
case class Int32Column(segments: Vector[SegmentInt])
    extends Column[Int32.type] {
  val dataType = Int32
}
case class Int64Column(segments: Vector[SegmentLong])
    extends Column[Int64.type] {
  val dataType = Int64
}
object Column {
  implicit val codec: JsonValueCodec[Column[_]] = JsonCodecMaker.make
  def apply[D <: DataType](tpe0: D)(
      segments0: Vector[tpe0.SegmentType]
  ): D#ColumnType =
    tpe0.makeColumn(segments0)

  def cast[D <: DataType](tpe: D)(segments: Vector[Segment[_]]): D#ColumnType =
    tpe.makeColumn(segments.map(tpe.cast))

}

case class SegmentInt(values: Array[Int]) extends Segment[Int32.type] {

  override def buffer = BufferInt(values)

  override def statistic: Statistic[Elem] = ???

  // def pair(other: this.type) : Int = 0

}
case class BufferInt(values: Array[Int]) extends Buffer[Int32.type] {

  override def toSeq: Seq[Int] = values.toSeq

  override def toSegment: SegmentInt = ???

}

case object Int64 extends DataType {

  type Elem = Long
  type BufferType = BufferLong
  type SegmentType = SegmentLong
  type ColumnType = Int64Column
  def makeColumn(s: Vector[SegmentType]): ColumnType = Int64Column(s)

}
case class SegmentLong(values: Array[Long]) extends Segment[Int64.type] {

  // override def pair(other: this.type): Int = ???


  override def buffer = BufferLong(values)

  override def statistic: Statistic[Elem] = ???


}
case class BufferLong(values: Array[Long]) extends Buffer[Int64.type] {

  override def toSeq = values.toSeq

  override def toSegment: SegmentLong = ???

}

object Segment {
  implicit val codec: JsonValueCodec[Segment[_]] = JsonCodecMaker.make

}

object Test {
  val b1 = BufferInt(Array(1))
  val b2 = BufferInt(Array(2))
  val segment1 = b1.toSegment
  val segment2 = b2.toSegment
  val tpe = Int32
  // val c = Column.make(tpe)(Vector(segment1, segment2))
  // val d = c.z
  implicitly[JsonValueCodec[Segment[_]]]
  implicitly[JsonValueCodec[Column[_]]]

  // def a[D<:DataType](a: D#SegmentType,b: D#SegmentType)(implicit ev: a.type =:=  D#SegmentType) = {
    
  //   a.pair()
  // }

  // a[Int32.type](segment1,segment2)

  // val columns: List[Column[_]] = List(c, c)

  // columns.map { column =>
  //   val cs  = column.segments.map { segment =>
  //     // m1[column.DType,column.SegType](
  //     //   segment,

  //     // )
  //     m2(segment)
  //   }
  //   cs
  // }

  def m1[D <: DataType, S <: Segment[D]](
      input: S
  ): S = ???
  def m2[D <: DataType](
      input: Segment[D]
  ): Segment[D] = ???
}
