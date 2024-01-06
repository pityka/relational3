package test
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
// sealed trait DataType { self: DataType =>

//   type Elem
//   type SegmentType <: Segment {
//     type D = self.type
//   }
//   type BufferType <: Buffer {
//     type D = self.type
//   }
//   type ColumnType <: Column {
//     type D = self.type
//   }
//   def cast(s: Segment) = s.asInstanceOf[SegmentType]
//   def makeColumn(s: Vector[SegmentType]): ColumnType
//   // def cast(s:Buffer[_]) : BufferType

// }
sealed trait Buffer {  self =>
  type Elem 
  type BufferType >: this.type <: Buffer
  type SegmentType <: Segment {
    type BufferType = self.BufferType 
    type SegmentType = self.SegmentType
    type Elem = self.Elem
  }
  type ColumnType <: Column {
    type Elem = self.Elem
    type BufferType = self.BufferType 
    type SegmentType = self.SegmentType
  }
  // val dataType: DataType
  def toSeq: Seq[Elem]
  def toSegment: SegmentType
  // def both[B<:Buffer](other: B)(implicit ev: B =:= BufferType): BufferType
  def both(other: BufferType): BufferType
}
sealed trait Segment { self =>
  // val dataType: DataType
  type SegmentType >: this.type <: Segment
  type Elem 
  type BufferType <: Buffer {
    type Elem = self.Elem 
    type SegmentType = self.SegmentType
    type BufferType = self.BufferType
  }
  type ColumnType <: Column {
    type Elem = self.Elem
    type BufferType = self.BufferType 
    type SegmentType = self.SegmentType
  }
  
  def buffer: BufferType
  def statistic: Statistic[Elem]
  // def pair(other: DType#SegmentType) : Int
  // def cast[D<:DataType] = this.asInstanceOf[Segment[D]]
}
sealed trait Column { self =>
  // type SegType = DType#SegmentType
  type ColumnType >: this.type <: Column 
  type Elem 
  type BufferType <: Buffer {
    type Elem = self.Elem     
    type BufferType = self.BufferType
    type SegmentType = self.SegmentType
  }
  type SegmentType <: Segment {
    type Elem = self.Elem     
    type BufferType = self.BufferType
    type SegmentType = self.SegmentType
  }
  
  
  def segments: Vector[SegmentType]
  // val z: Seq[Elem]
  // val x = segments.head:Segment[DType]
  
  def ++(other: ColumnType)  : ColumnType 
  
}

sealed trait Statistic[T]
// case object Int32 extends DataType {

//   // override def cast(s: Segment[_]): SegmentType = s.asInstanceOf[SegmentInt]

//   type Elem = Int
//   type BufferType = BufferInt
//   type SegmentType = SegmentInt
//   type ColumnType = Int32Column
//   def makeColumn(s: Vector[SegmentType]): ColumnType = Int32Column(s)

// }
case class Int32Column(segments: Vector[SegmentInt]) extends Column {

  override def ++(other: Int32Column): Int32Column = Int32Column(segments ++ other.segments)


  type Elem = Int 
  type BufferType = BufferInt 
  type SegmentType = SegmentInt 
  type ColumnType = Int32Column



}

object Column {
  implicit val codec: JsonValueCodec[Column] = JsonCodecMaker.make
  // def apply(
  //     segments: Vector[tpe.SegmentType]
  // ) =
  //   tpe.makeColumn(segments)

  // def cast[S <: Segment](segments: Vector[S]): D#ColumnType =
  //   tpe.makeColumn(segments.map(tpe.cast))

}

case class SegmentInt(values: Array[Int]) extends Segment {
   type Elem = Int 
  type BufferType = BufferInt 
  type SegmentType = SegmentInt 
  type ColumnType = Int32Column

  override def buffer = BufferInt(values)

  override def statistic: Statistic[Elem] = ???

  // def pair(other: this.type) : Int = 0

}
case class BufferInt(values: Array[Int]) extends Buffer {

   type Elem = Int 
  type BufferType = BufferInt 
  type SegmentType = SegmentInt 
  type ColumnType = Int32Column

  def both(other: BufferType): BufferType = ???

  override def toSeq: Seq[Int] = values.toSeq

  override def toSegment: SegmentInt = ???

  def filterInEquality[A<:Buffer,B<:Buffer](
      comparison: A)(
      cutoff: B
  )(implicit ev: B =:= comparison.BufferType): Unit = {
    val idx = comparison.both(ev(cutoff))

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
  // val tpe = Int32
  val c : Column = Int32Column(Vector(segment1, segment2))
  // val d = c.z
  val t : Vector[SegmentInt] = Vector(segment1.buffer.toSegment , segment1)
  implicitly[JsonValueCodec[Segment]]
  implicitly[JsonValueCodec[Column]]
  val t0  = c.segments 
  val t1  = c.segments.map(_.buffer)
  t1.reduce((a,b) => {
    val r = a.both(b)
    r
  })

  segment1.buffer.filterInEquality(segment1.buffer)(segment2.buffer)
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

  def m1[ S <: Segment](
      input: S
  ): S = ???
  def m2(
      input: Segment
  ): Segment = ???
}
