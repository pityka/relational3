package ra3
import tasks.*
import cats.effect.IO
import java.nio.ByteOrder
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
private[ra3] sealed trait TaggedSegment { self =>
  type SegmentType <: Segment
  val tag: ColumnTag {
    type SegmentType = self.SegmentType
  }
  def segment: tag.SegmentType

}
private[ra3] object TaggedSegment {
  // $COVERAGE-OFF$
  implicit val codec: JsonValueCodec[TaggedSegment] = JsonCodecMaker.make
  // $COVERAGE-ON$

}
private[ra3] sealed trait TaggedSegments { self =>
  type SegmentType <: Segment
  val tag: ColumnTag {
    type SegmentType = self.SegmentType
  }
  def segments: Seq[tag.SegmentType]

}
private[ra3] object TaggedSegments {
  // $COVERAGE-ON$
  implicit val codec: JsonValueCodec[TaggedSegments] = JsonCodecMaker.make
  // $COVERAGE-OFF$

}

private[ra3] case class TaggedSegmentsI32(segments: Seq[SegmentInt])
    extends TaggedSegments {
  val tag = ColumnTag.I32
  type SegmentType = SegmentInt
  type Elem = Int
}
private[ra3] case class TaggedSegmentsI64(segments: Seq[SegmentLong])
    extends TaggedSegments {
  val tag = ColumnTag.I64
  type SegmentType = SegmentLong
  type Elem = Long
}
private[ra3] case class TaggedSegmentsF64(segments: Seq[SegmentDouble])
    extends TaggedSegments {
  val tag = ColumnTag.F64
  type SegmentType = SegmentDouble
  type Elem = Double
}
private[ra3] case class TaggedSegmentsString(segments: Seq[SegmentString])
    extends TaggedSegments {
  val tag = ColumnTag.StringTag
  type SegmentType = SegmentString
  type Elem = CharSequence
}
private[ra3] case class TaggedSegmentsInstant(segments: Seq[SegmentInstant])
    extends TaggedSegments {
  val tag = ColumnTag.Instant
  type SegmentType = SegmentInstant
  type Elem = Long
}

private[ra3] sealed trait Segment { self =>
  // type Elem

  // def buffer(implicit tsc: TaskSystemComponents): IO[BufferType]

  def numElems: Int
  def numBytes: Long

}

private[ra3] object Segment {

  case class GroupMap(
      map: SegmentInt,
      numGroups: Int,
      groupSizes: SegmentInt
  )

  import com.github.plokhotnyuk.jsoniter_scala.core.*
  import com.github.plokhotnyuk.jsoniter_scala.macros.*
  implicit val customCodecOfDouble: JsonValueCodec[Double] =
    Utils.customDoubleCodec

  // $COVERAGE-OFF$
  implicit val segmentCodec: JsonValueCodec[Segment] = JsonCodecMaker.make
  // $COVERAGE-ON$

}

private[ra3] final case class SegmentDouble(
    sf: Option[SharedFile],
    numElems: Int,
    statistic: StatisticDouble
) extends Segment
    with TaggedSegment {
  def numBytes = numElems * 8
  type Elem = Double
  type SegmentType = SegmentDouble
  val tag: ColumnTag.F64.type = ColumnTag.F64
  def segment = this
  def nonMissingMinMax: Option[(Double, Double)] = statistic.nonMissingMinMax

  def buffer(implicit tsc: TaskSystemComponents) =
    sf match {
      case None if statistic.constantValue.isDefined =>
        IO.pure(
          BufferDouble.constant(
            value = statistic.constantValue.get,
            length = numElems
          )
        )
      case None => IO.pure(BufferDouble(Array.empty[Double]))
      case Some(sf) =>
        sf.bytes.map { byteVector =>
          val bb =
            Utils
              .decompress(byteVector)
              .order(ByteOrder.LITTLE_ENDIAN)
              .asDoubleBuffer()
          val ar = Array.ofDim[Double](bb.remaining)
          bb.get(ar)
          BufferDouble(ar)
        }
    }

}

private[ra3] final case class SegmentInt(
    sf: Option[SharedFile],
    numElems: Int,
    statistic: StatisticInt
) extends Segment
    with TaggedSegment {
  def numBytes = numElems * 4
  type Elem = Int
  type SegmentType = SegmentInt
  val tag: ColumnTag.I32.type = ColumnTag.I32
  def segment = this
  def nonMissingMinMax = statistic.nonMissingMinMax

  def buffer(implicit tsc: TaskSystemComponents): IO[BufferInt] = {
    sf match {
      case None if statistic.constantValue.isDefined =>
        IO.pure(
          BufferInt.constant(
            value = statistic.constantValue.get,
            length = numElems
          )
        )

      case None =>
        assert(numElems == 0)
        IO.pure(BufferInt.empty)
      case Some(value) =>
        value.bytes.map { byteVector =>
          val bb =
            Utils
              .decompress(byteVector)
              .order(ByteOrder.LITTLE_ENDIAN)
              .asIntBuffer()
          val ar = Array.ofDim[Int](bb.remaining)
          bb.get(ar)
          BufferInt(ar)
        }
    }

  }

  def isConstant(i: Int) =
    sf.isEmpty && statistic.constantValue.exists(_ == i)

}
private[ra3] final case class SegmentLong(
    sf: Option[SharedFile],
    numElems: Int,
    statistic: StatisticLong
) extends Segment
    with TaggedSegment {
  def numBytes = numElems * 8

  def nonMissingMinMax = statistic.nonMissingMinMax

  type Elem = Long
  type SegmentType = SegmentLong
  val tag: ColumnTag.I64.type = ColumnTag.I64
  def segment = this

  def buffer(implicit tsc: TaskSystemComponents): IO[BufferLong] = {
    sf match {
      case None if statistic.constantValue.isDefined =>
        IO.pure(
          BufferLong.constant(
            value = statistic.constantValue.get,
            length = numElems
          )
        )
      case None => IO.pure(BufferLong(Array.emptyLongArray))
      case Some(value) =>
        value.bytes.map { byteVector =>
          val bb =
            Utils
              .decompress(byteVector)
              .order(ByteOrder.LITTLE_ENDIAN)
              .asLongBuffer()
          val ar = Array.ofDim[Long](bb.remaining)
          bb.get(ar)
          BufferLong(ar)
        }
    }

  }

}
private[ra3] final case class SegmentInstant(
    sf: Option[SharedFile],
    numElems: Int,
    statistic: StatisticLong
) extends Segment
    with TaggedSegment {
  def numBytes = numElems * 8
  def nonMissingMinMax = statistic.nonMissingMinMax

  type Elem = Long
  type SegmentType = SegmentInstant
  val tag: ColumnTag.Instant.type = ColumnTag.Instant
  def segment = this

  def buffer(implicit tsc: TaskSystemComponents): IO[BufferInstant] = {
    sf match {
      case None if statistic.constantValue.isDefined =>
        IO.pure(
          BufferInstant.constant(
            value = statistic.constantValue.get,
            length = numElems
          )
        )
      case None => IO.pure(BufferInstant(Array.emptyLongArray))
      case Some(value) =>
        value.bytes.map { byteVector =>
          val bb = Utils
            .decompress(byteVector)
            .order(ByteOrder.LITTLE_ENDIAN)
            .asLongBuffer()
          val ar = Array.ofDim[Long](bb.remaining)
          bb.get(ar)
          BufferInstant(ar)
        }
    }

  }

}
private[ra3] final case class SegmentString(
    sf: Option[SharedFile],
    numElems: Int,
    numBytes: Long,
    statistic: StatisticCharSequence
) extends Segment
    with TaggedSegment {
  def nonMissingMinMax = statistic.nonMissingMinMax

  type Elem = CharSequence
  type SegmentType = SegmentString
  val tag: ColumnTag.StringTag.type = ColumnTag.StringTag
  def segment = this
  def buffer(implicit tsc: TaskSystemComponents): IO[BufferString] = {
    sf match {
      case None if statistic.constantValue.isDefined =>
        IO.pure(
          BufferString.constant(
            value = statistic.constantValue.get.toString,
            length = numElems
          )
        )
      case None => IO.pure(BufferString(Array.empty[CharSequence]))
      case Some(value) =>
        value.bytes.map { byteVector =>
          val decompressed = Utils.decompress(byteVector)

          val bb =
            decompressed
              .order(ByteOrder.BIG_ENDIAN) // char data is laid out big endian
          val ar = Array.ofDim[CharSequence](numElems)
          var i = 0
          while (i < numElems) {
            val len = bb.getInt()
            val char = bb.slice(bb.position(), len * 2).asCharBuffer()
            ar(i) = char
            bb.position(bb.position() + len * 2)
            i += 1
          }

          BufferString(ar)
        }
    }

  }

}
