package test
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
sealed trait DataType { self: DataType =>

  type Elem
  type SegmentType <: Segment {
    type D = self.type
  }
  type BufferType <: Buffer {
    type D = self.type
  }
  type ColumnType <: Column {
    type D = self.type
  }
  def cast(s: Segment) = s.asInstanceOf[SegmentType]
  def makeColumn(s: Vector[SegmentType]): ColumnType
  // def cast(s:Buffer[_]) : BufferType

}
sealed trait Buffer { // self: DType#BufferType =>
  type D <: DataType 
  // val dataType: DataType
  def toSeq: Seq[D#Elem]
  def toSegment: D#SegmentType
  def both(other: D#BufferType): D#BufferType
}
sealed trait Segment { self =>
  // val dataType: DataType
  type D<:DataType
  type Elem = D#Elem
  type SegmentType = D#SegmentType
  type BufferType = D#BufferType 
  def buffer: BufferType
  def statistic: Statistic[Elem]
  // def pair(other: DType#SegmentType) : Int
  // def cast[D<:DataType] = this.asInstanceOf[Segment[D]]
}
sealed trait Column { self =>
  // type SegType = DType#SegmentType
  type D <: DataType
  val dataType: D
  
  def segments: Vector[dataType.SegmentType]
  // val z: Seq[Elem]
  // val x = segments.head:Segment[DType]
  
  def ++(other: self.type) = {
    val both  : Vector[dataType.SegmentType]= segments ++ other.segments
    Column(dataType)(both)
  }
}

sealed trait Statistic[T]
case object Int32 extends DataType {

  // override def cast(s: Segment[_]): SegmentType = s.asInstanceOf[SegmentInt]

  type Elem = Int
  type BufferType = BufferInt
  type SegmentType = SegmentInt
  type ColumnType = Int32Column
  def makeColumn(s: Vector[SegmentType]): ColumnType = Int32Column(s)

}
case class Int32Column(segments: Vector[SegmentInt]) extends Column {

  override val dataType: Int32.type = Int32




  type D = Int32.type
}

object Column {
  implicit val codec: JsonValueCodec[Column] = JsonCodecMaker.make
  def apply[D<:DataType](tpe: D)(
      segments: Vector[tpe.SegmentType]
  ) =
    tpe.makeColumn(segments)

  def cast[D <: DataType](tpe: D)(segments: Vector[D#SegmentType]): D#ColumnType =
    tpe.makeColumn(segments.map(tpe.cast))

}

case class SegmentInt(values: Array[Int]) extends Segment {
  type D = Int32.type
  override def buffer = BufferInt(values)

  override def statistic: Statistic[Elem] = ???

  // def pair(other: this.type) : Int = 0

}
case class BufferInt(values: Array[Int]) extends Buffer {

  type D = Int32.type
  override def both(other: BufferInt): BufferInt = ???

  override def toSeq: Seq[Int] = values.toSeq

  override def toSegment: SegmentInt = ???

  def filterInEquality[D2<:DataType](
      cutoff: D2#BufferType,
      comparison: D2#BufferType
  ): Unit = {
    val idx = comparison.both(cutoff.asInstanceOf[comparison.D#BufferType])

    println(idx)

  }

}

// case object Int64 extends DataType {
 
//   type Elem = Long
//   type BufferType = BufferLong
//   type SegmentType = SegmentLong
//   type ColumnType = Int64Column
//   def makeColumn(s: Vector[SegmentType]): ColumnType = Int64Column(s)

// }
// case class Int64Column(segments: Vector[SegmentLong]) extends Column {

//   val dataType = Int64
// }
// case class SegmentLong(values: Array[Long]) extends Segment {

//   // override def pair(other: this.type): Int = ???
//   val dataType = Int64

//   override def buffer = BufferLong(values)

//   override def statistic: Statistic[Elem] = ???

// }
// case class BufferLong(values: Array[Long]) extends Buffer {

//   override def toSeq: Seq[Long] = values.toSeq

//   override def toSegment: SegmentLong = SegmentLong(values)

//   override def both(other: BufferLong): Unit = ???

//   val dataType = Int64

// }

object Segment {
  implicit val codec: JsonValueCodec[Segment] = JsonCodecMaker.make

}

object Test {
  val b1 = BufferInt(Array(1))
  val b2 = BufferInt(Array(2))
  val segment1  = b1.toSegment
  val segment2 = b2.toSegment
  val tpe = Int32
  val c : Column = tpe.makeColumn(Vector(segment1, segment2))
  // val d = c.z
  val t : Vector[SegmentInt] = Vector(segment1.buffer.toSegment , segment1)
  implicitly[JsonValueCodec[Segment]]
  implicitly[JsonValueCodec[Column]]
  val t0 : Vector[c.dataType.SegmentType] = c.segments 
  val t1 : Vector[c.dataType.SegmentType#BufferType] = c.segments.map(_.buffer)
  t1.reduce((a,b) => {
    val r : c.dataType.SegmentType#BufferType = a.both(b)
    r
  })

  segment1.buffer.filterInEquality(segment1.buffer,segment2.buffer)
  // List(segment1,segment2)

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

  def m1[D <: DataType, S <: Segment](
      input: S
  ): S = ???
  def m2[D <: DataType](
      input: Segment
  ): Segment = ???
}
