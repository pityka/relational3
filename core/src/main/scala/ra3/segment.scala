package ra3
import tasks.*
import cats.effect.IO
import java.nio.ByteOrder
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import java.nio.ByteBuffer
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

  def wrap: Box[?]

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

private[ra3] object SegmentDouble {
  def apply(
      sf: Option[SharedFile],
      numElems: Int,
      statistic: StatisticDouble
  ): SegmentDouble = SegmentDouble(sf, numElems, statistic, None)
}
private[ra3] final case class SegmentDouble(
    sf: Option[SharedFile],
    numElems: Int,
    statistic: StatisticDouble,
    parquet: Option[ra3.parquet.ParquetSegmentRef]
) extends Segment
    with TaggedSegment {

  def wrap: Box[?] = F64Var(Right(Seq(this)))
  def numBytes = numElems * 8
  type Elem = Double
  type SegmentType = SegmentDouble
  val tag: ColumnTag.F64.type = ColumnTag.F64
  def segment = this
  def nonMissingMinMax: Option[(Double, Double)] = statistic.nonMissingMinMax

  def buffer(implicit tsc: TaskSystemComponents): IO[BufferDouble] =
    parquet match {
      case Some(ref) =>
        ref.decode.map(vs =>
          BufferDouble(vs.map(_.fold(Double.NaN)(_.asInstanceOf[Double])).toArray)
        )
      case None =>
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
        IO.cede >> sf.bytes
          .map { byteVector =>
            val bb =
              Utils
                .decompress(byteVector, skipCompress = true)
                .order(ByteOrder.LITTLE_ENDIAN)
                .asDoubleBuffer()
            val ar = Array.ofDim[Double](bb.remaining)
            bb.get(ar)
            BufferDouble(ar)
          }
          .logElapsed
          .countInF64(numElems, sf.byteSize)
          .guarantee(IO.cede)

    }
    }

}

private[ra3] object SegmentInt {
  def apply(
      sf: Option[SharedFile],
      numElems: Int,
      statistic: StatisticInt
  ): SegmentInt = SegmentInt(sf, numElems, statistic, None)
}
private[ra3] final case class SegmentInt(
    sf: Option[SharedFile],
    numElems: Int,
    statistic: StatisticInt,
    parquet: Option[ra3.parquet.ParquetSegmentRef]
) extends Segment
    with TaggedSegment {

  def wrap: Box[?] = I32Var(Right(Seq(this)))
  def numBytes = numElems * 4
  type Elem = Int
  type SegmentType = SegmentInt
  val tag: ColumnTag.I32.type = ColumnTag.I32
  def segment = this
  def nonMissingMinMax = statistic.nonMissingMinMax

  def buffer(implicit tsc: TaskSystemComponents): IO[BufferInt] = {
    parquet match {
      case Some(ref) =>
        ref.decode.map(vs =>
          BufferInt(vs.map(_.fold(Int.MinValue)(_.asInstanceOf[Int])).toArray)
        )
      case None =>
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
        IO.cede >> value.bytes
          .map { byteVector =>
            val bb =
              Utils
                .decompress(byteVector, skipCompress=true)
                .order(ByteOrder.LITTLE_ENDIAN)
                .asIntBuffer()
            val ar = Array.ofDim[Int](bb.remaining)
            bb.get(ar)
            BufferInt(ar)
          }
          .logElapsed
          .countInI32(numElems, value.byteSize)
          .guarantee(IO.cede)
    }
    }

  }

  def isConstant(i: Int) =
    sf.isEmpty && statistic.constantValue.exists(_ == i)

}
private[ra3] object SegmentLong {
  def apply(
      sf: Option[SharedFile],
      numElems: Int,
      statistic: StatisticLong
  ): SegmentLong = SegmentLong(sf, numElems, statistic, None)
}
private[ra3] final case class SegmentLong(
    sf: Option[SharedFile],
    numElems: Int,
    statistic: StatisticLong,
    parquet: Option[ra3.parquet.ParquetSegmentRef]
) extends Segment
    with TaggedSegment {

  def wrap: Box[?] = I64Var(Right(Seq(this)))
  def numBytes = numElems * 8

  def nonMissingMinMax = statistic.nonMissingMinMax

  type Elem = Long
  type SegmentType = SegmentLong
  val tag: ColumnTag.I64.type = ColumnTag.I64
  def segment = this

  def buffer(implicit tsc: TaskSystemComponents): IO[BufferLong] = {
    parquet match {
      case Some(ref) =>
        ref.decode.map(vs =>
          BufferLong(vs.map(_.fold(Long.MinValue)(_.asInstanceOf[Long])).toArray)
        )
      case None =>
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
        IO.cede >> value.bytes
          .map { byteVector =>
            val bb =
              Utils
                .decompress(byteVector)
                .order(ByteOrder.LITTLE_ENDIAN)
                .asLongBuffer()
            val ar = Array.ofDim[Long](bb.remaining)
            bb.get(ar)
            BufferLong(ar)
          }
          .logElapsed
          .countInI64(numElems, value.byteSize)
          .guarantee(IO.cede)
    }
    }

  }

}
private[ra3] object SegmentInstant {
  def apply(
      sf: Option[SharedFile],
      numElems: Int,
      statistic: StatisticLong
  ): SegmentInstant = SegmentInstant(sf, numElems, statistic, None)
}
private[ra3] final case class SegmentInstant(
    sf: Option[SharedFile],
    numElems: Int,
    statistic: StatisticLong,
    parquet: Option[ra3.parquet.ParquetSegmentRef]
) extends Segment
    with TaggedSegment {

  def wrap: Box[?] = InstVar(Right(Seq(this)))
  def numBytes = numElems * 8
  def nonMissingMinMax = statistic.nonMissingMinMax

  type Elem = Long
  type SegmentType = SegmentInstant
  val tag: ColumnTag.Instant.type = ColumnTag.Instant
  def segment = this

  def buffer(implicit tsc: TaskSystemComponents): IO[BufferInstant] = {
    parquet match {
      case Some(ref) =>
        ref.decode.map(vs =>
          BufferInstant(vs.map(_.fold(Long.MinValue)(_.asInstanceOf[Long])).toArray)
        )
      case None =>
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
        IO.cede >> value.bytes
          .map { byteVector =>
            val bb = Utils
              .decompress(byteVector)
              .order(ByteOrder.LITTLE_ENDIAN)
              .asLongBuffer()
            val ar = Array.ofDim[Long](bb.remaining)
            bb.get(ar)
            BufferInstant(ar)
          }
          .logElapsed
          .countInInst(numElems, value.byteSize)
          .guarantee(IO.cede)
    }
    }

  }

}

private[ra3] object SegmentString {
  def apply(
      sf: Option[ra3.segmentstring.SerializationType],
      numElems: Int,
      numBytes: Long,
      statistic: StatisticCharSequence
  ): SegmentString = SegmentString(sf, numElems, numBytes, statistic, None)
}
private[ra3] final case class SegmentString(
    sf: Option[ra3.segmentstring.SerializationType],
    numElems: Int,
    numBytes: Long,
    statistic: StatisticCharSequence,
    parquet: Option[ra3.parquet.ParquetSegmentRef]
) extends Segment
    with TaggedSegment {
  def nonMissingMinMax = statistic.nonMissingMinMax

  def wrap: Box[?] = StrVar(Right(Seq(this)))

  type Elem = CharSequence
  type SegmentType = SegmentString
  val tag: ColumnTag.StringTag.type = ColumnTag.StringTag
  def segment = this
  def buffer(implicit tsc: TaskSystemComponents): IO[BufferString] = {
    parquet match {
      case Some(ref) =>
        ref.decode.map(vs =>
          BufferString(
            vs.map(
              _.fold(BufferString.missing)(v =>
                (v.asInstanceOf[String]: CharSequence)
              )
            ).toArray
          )
        )
      case None =>
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
        value.buffer
    }
    }

  }

}
