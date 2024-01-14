package ra3
import tasks._
import cats.effect.IO
import java.nio.ByteOrder
sealed trait Segment { self =>
  type Elem
  type SegmentType >: this.type <: Segment  {
    type Elem = self.Elem
    type BufferType = self.BufferType
  }
  def asSegmentType = this.asInstanceOf[SegmentType]
  type BufferType <: Buffer {
    type Elem = self.Elem
    type SegmentType = self.SegmentType
    type BufferType = self.BufferType
  }
  type ColumnType <: Column {
    type Elem = self.Elem
    type BufferType = self.BufferType
    type SegmentType = self.SegmentType
    type ColumnTagType = self.ColumnTagType
  }
  type ColumnTagType <: ColumnTag {
    type ColumnType = self.ColumnType
    type SegmentType = self.SegmentType
    type BufferType = self.BufferType
    type Elem = self.Elem
  }
  def tag: ColumnTagType
  def buffer(implicit tsc: TaskSystemComponents): IO[BufferType]
  // def statistics: IO[Option[Statistic[Elem]]]
  def numElems: Int
  def minMax: Option[(Elem,Elem)] 

  
  def as(c: Column) = this.asInstanceOf[c.SegmentType]
  def as(c: Segment) = this.asInstanceOf[c.SegmentType]
  def as[S <: Segment { type SegmentType = S }] = this.asInstanceOf[S]
  def as(c: ColumnTag) = this.asInstanceOf[c.SegmentType]
}

object Segment {

  case class GroupMap(
      map: SegmentInt,
      numGroups: Int,
      groupSizes: SegmentInt
  )

}

sealed trait SegmentPair { self =>
  type SegmentPairType >: this.type <: SegmentPair
  type Elem
  type BufferType <: Buffer {
    type Elem = self.Elem
    type SegmentType = self.SegmentType
    type BufferType = self.BufferType
  }
  type SegmentType <: Segment {
    type BufferType = self.BufferType
    type SegmentType = self.SegmentType
    type Elem = self.Elem
  }
  def a: SegmentType
  def b: SegmentType
}
case class I32Pair(a: SegmentInt, b: SegmentInt) extends SegmentPair {
  type Elem = Int
  type BufferType = BufferInt
  type SegmentType = SegmentInt
  type ColumnType = Column.Int32Column
}
case class F64Pair(a: SegmentDouble, b: SegmentDouble) extends SegmentPair {
  type Elem = Double
  type BufferType = BufferDouble
  type SegmentType = SegmentDouble
  type ColumnType = Column.F64Column
}

final case class SegmentDouble(sf: Option[SharedFile], numElems: Int, minMax: Option[(Double,Double)] ) extends Segment {
  type Elem = Double
  type BufferType = BufferDouble
  type SegmentType = SegmentDouble
  type ColumnType = Column.F64Column
  type ColumnTagType = ColumnTag.F64.type
  val tag = ColumnTag.F64


  override def buffer(implicit tsc: TaskSystemComponents) =
    sf match {
      case None => IO.pure(BufferDouble(Array.empty[Double]))
      case Some(sf) =>  
          sf.bytes.map { byteVector =>
      val bb =
        byteVector.toByteBuffer.order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer()
      val ar = Array.ofDim[Double](bb.remaining)
      bb.get(ar)
      BufferDouble(ar)
    }
    }


  


}
final case class SegmentInt(sf: Option[SharedFile], numElems: Int, minMax: Option[(Int,Int)] ) extends Segment {

  type Elem = Int
  type BufferType = BufferInt
  type SegmentType = SegmentInt
  type ColumnType = Column.Int32Column
  type ColumnTagType = ColumnTag.I32.type
  val tag = ColumnTag.I32

  // override def pair(other: this.type) = I32Pair(this,other)

  override def buffer(implicit tsc: TaskSystemComponents): IO[BufferInt] = {
    sf match {
      case None =>  IO.pure(BufferInt(Array.emptyIntArray))
      case Some(value) =>
           value.bytes.map { byteVector =>
      val bb =
        byteVector.toByteBuffer.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer()
      val ar = Array.ofDim[Int](bb.remaining)
      bb.get(ar)
      BufferInt(ar)
    }
    }

   

  }

  def statistics: IO[Option[Statistic[Int]]] = IO.pure(None)

}
