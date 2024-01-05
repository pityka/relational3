package ra3
import tasks._ 
import cats.effect.IO
import java.nio.ByteOrder

sealed trait Segment[DType<:DataType] { //self : DType#SegmentType =>
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

final case class SegmentDouble(sf: SharedFile, numElems: Int)
    extends Segment[F64] {

  override def buffer(implicit tsc: TaskSystemComponents) =
    ???

  override def statistics = ???

}
final case class SegmentInt(sf: SharedFile, numElems: Int)
    extends Segment[Int32] {

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