package ra3
import tasks._
import cats.effect.IO
import java.nio.ByteOrder
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
sealed trait Segment { self =>
  // type BufferType= DType#BufferType
  type DType <: DataType
  val dataType: DataType 
  type Elem = DType#Elem
  type BufferType = DType#BufferType
  type SegmentType = DType#SegmentType
  def buffer(implicit tsc: TaskSystemComponents): IO[BufferType]
  def statistics: IO[Option[Statistic[Elem]]]
  def numElems: Int
  def as[D <: DataType] = this.asInstanceOf[D#SegmentType]
}

object Segment {

  case class GroupMap(
      map: SegmentInt,
      numGroups: Int,
      groupSizes: SegmentInt
  )

  implicit val codec: JsonValueCodec[Segment] = JsonCodecMaker.make
}

sealed trait SegmentPair {
  type DType <: DataType
  def a: DType#SegmentType
  def b: DType#SegmentType
}
case class I32Pair(a: SegmentInt, b: SegmentInt)
    extends SegmentPair {
  type DType = Int32.type
}
case class F64Pair(a: SegmentDouble, b: SegmentDouble)
    extends SegmentPair {
  type DType = F64.type
}

final case class SegmentDouble(sf: SharedFile, numElems: Int)
    extends Segment {

  val dataType = F64
  type DType = F64.type

  // override def pair(other: this.type) = F64Pair(this,other)

  override def canEqual(that: Any): Boolean = ???

  override def buffer(implicit tsc: TaskSystemComponents) =
    ???

  override def statistics = ???

  // def pair(other: SegmentDouble) = F64Pair(this,other)

}
final case class SegmentInt(sf: SharedFile, numElems: Int)
    extends Segment {


  val dataType = Int32
  type DType = Int32.type

  // override def pair(other: this.type) = I32Pair(this,other)

  override def buffer(implicit tsc: TaskSystemComponents): IO[BufferInt] = {
    // import fs2.interop.scodec.StreamDecoder
    // import scodec.codecs._
    // sf.stream.through(StreamDecoder.many(scodec.codecs.int32L).toPipeByte).chunkAll.compile.last.map{_.get.toArray}

    sf.bytes.map { byteVector =>
      val bb =
        byteVector.toByteBuffer.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer()
      val ar = Array.ofDim[Int](bb.remaining / 4)
      bb.get(ar)
      BufferInt(ar)
    }

  }

  def statistics: IO[Option[Statistic[Int]]] = IO.pure(None)

}
object SegmentInt {
  implicit val codec: JsonValueCodec[SegmentInt] = JsonCodecMaker.make
}
