package ra3
import tasks._ 
import cats.effect.IO
import java.nio.ByteOrder

sealed trait Segment[DType<:DataType] { self  =>
  // type BufferType= DType#BufferType
  type Elem = DType#Elem
  def buffer(implicit tsc: TaskSystemComponents): IO[DType#BufferType]
  def statistics: IO[Option[Statistic[Elem]]]
  def numElems: Int
  def as[D<:DataType] = this.asInstanceOf[Segment[D]]
}

object Segment {

  case class GroupMap(
      map: SegmentInt,
      numGroups: Int,
      groupSizes: SegmentInt
  )

  import com.github.plokhotnyuk.jsoniter_scala.macros._
  import com.github.plokhotnyuk.jsoniter_scala.core._
  implicit val codec: JsonValueCodec[Segment[_]] = JsonCodecMaker.make
}

sealed trait SegmentPair[S <: Segment[_]] {
  def a: S
  def b: S
}
case class I32Pair(a: SegmentInt, b: SegmentInt) extends SegmentPair[SegmentInt]
case class F64Pair(a: SegmentDouble, b: SegmentDouble)
    extends SegmentPair[SegmentDouble]

final case class SegmentDouble(sf: SharedFile, numElems: Int)
    extends Segment[F64] {

  // override def pair(other: this.type) = F64Pair(this,other)

  override def canEqual(that: Any): Boolean = ???


  override def buffer(implicit tsc: TaskSystemComponents) =
    ???

  override def statistics = ???

  // def pair(other: SegmentDouble) = F64Pair(this,other)

}
final case class SegmentInt(sf: SharedFile, numElems: Int)
    extends Segment[Int32] {

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