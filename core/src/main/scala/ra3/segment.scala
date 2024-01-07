package ra3
import tasks._
import cats.effect.IO
import java.nio.ByteOrder
sealed trait Segment { self =>
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
    type ColumnTagType = self.ColumnTagType
  }
   type ColumnTagType <: ColumnTag {
    type ColumnType = self.ColumnType
    type SegmentType = self.SegmentType
    type BufferType = self.BufferType
    type Elem = self.Elem
  }
  def tag : ColumnTagType
  def buffer(implicit tsc: TaskSystemComponents): IO[BufferType]
  def statistics: IO[Option[Statistic[Elem]]]
  def numElems: Int
  def as(c:Column) = this.asInstanceOf[c.SegmentType]
  def as(c:Segment) = this.asInstanceOf[c.SegmentType]
  def as(c:ColumnTag) = this.asInstanceOf[c.SegmentType]
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
case class I32Pair(a: SegmentInt, b: SegmentInt)
    extends SegmentPair {
       type Elem = Int
  type BufferType = BufferInt
  type SegmentType = SegmentInt
  type ColumnType = Column.Int32Column
}
case class F64Pair(a: SegmentDouble, b: SegmentDouble)
    extends SegmentPair {
   type Elem = Double
  type BufferType = BufferDouble
  type SegmentType = SegmentDouble
  type ColumnType = Column.F64Column
}

final case class SegmentDouble(sf: SharedFile, numElems: Int)
    extends Segment {
 type Elem = Double
  type BufferType = BufferDouble
  type SegmentType = SegmentDouble
  type ColumnType = Column.F64Column
  type ColumnTagType = ColumnTag.F64.type 
  val tag = ColumnTag.F64

  // override def pair(other: this.type) = F64Pair(this,other)


  override def buffer(implicit tsc: TaskSystemComponents) =
    ???

  override def statistics = ???

  // def pair(other: SegmentDouble) = F64Pair(this,other)

}
final case class SegmentInt(sf: SharedFile, numElems: Int)
    extends Segment {

 type Elem = Int 
  type BufferType = BufferInt 
  type SegmentType = SegmentInt 
  type ColumnType = Column.Int32Column
  type ColumnTagType = ColumnTag.I32.type 
  val tag = ColumnTag.I32
  
  // override def pair(other: this.type) = I32Pair(this,other)

  override def buffer(implicit tsc: TaskSystemComponents): IO[BufferInt] = {
    // import fs2.interop.scodec.StreamDecoder
    // import scodec.codecs._
    // sf.stream.through(StreamDecoder.many(scodec.codecs.int32L).toPipeByte).chunkAll.compile.last.map{_.get.toArray}

    sf.bytes.map { byteVector =>
      val bb =
        byteVector.toByteBuffer.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer()
      val ar = Array.ofDim[Int](bb.remaining )
      bb.get(ar)
      BufferInt(ar)
    }

  }

  def statistics: IO[Option[Statistic[Int]]] = IO.pure(None)

}

