package ra3
import tasks._
import cats.effect.IO
import java.nio.ByteOrder
sealed trait Segment { self =>
  type Elem
  type SegmentType >: this.type <: Segment {
    type Elem = self.Elem
    type BufferType = self.BufferType
    type SegmentType = self.SegmentType
    type ColumnTagType = self.ColumnTagType
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
  def numElems: Int
  def nonMissingMinMax: Option[(Elem, Elem)]

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

  import com.github.plokhotnyuk.jsoniter_scala.core._
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  implicit val customCodecOfDouble: JsonValueCodec[Double] =Utils.customDoubleCodec
    
  implicit val segmentCodec: JsonValueCodec[Segment] = JsonCodecMaker.make

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
case class I64Pair(a: SegmentLong, b: SegmentLong) extends SegmentPair {
  type Elem = Long
  type BufferType = BufferLong
  type SegmentType = SegmentLong
  type ColumnType = Column.I64Column
}
case class InstantPair(a: SegmentInstant, b: SegmentInstant)
    extends SegmentPair {
  type Elem = Long
  type BufferType = BufferInstant
  type SegmentType = SegmentInstant
  type ColumnType = Column.InstantColumn
}
case class StringPair(a: SegmentString, b: SegmentString) extends SegmentPair {
  type Elem = CharSequence
  type BufferType = BufferString
  type SegmentType = SegmentString
  type ColumnType = Column.StringColumn
}

final case class SegmentDouble(
    sf: Option[SharedFile],
    numElems: Int,
    statistic: StatisticDouble
) extends Segment {
  type Elem = Double
  type BufferType = BufferDouble
  type SegmentType = SegmentDouble
  type ColumnType = Column.F64Column
  type ColumnTagType = ColumnTag.F64.type
  val tag = ColumnTag.F64

  def nonMissingMinMax: Option[(Double, Double)] = statistic.nonMissingMinMax

  override def buffer(implicit tsc: TaskSystemComponents) =
    sf match {
      case None if statistic.constantValue.isDefined =>
        IO.pure(BufferDouble.constant(value = statistic.constantValue.get, length = numElems))
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

final case class SegmentInt(
    sf: Option[SharedFile],
    numElems: Int,
    statistic: StatisticInt
) extends Segment {

  type Elem = Int
  type BufferType = BufferInt
  type SegmentType = SegmentInt
  type ColumnType = Column.Int32Column
  type ColumnTagType = ColumnTag.I32.type
  val tag = ColumnTag.I32

    def nonMissingMinMax= statistic.nonMissingMinMax


  override def buffer(implicit tsc: TaskSystemComponents): IO[BufferInt] = {
    sf match {
      case None if statistic.constantValue.isDefined =>
        IO.pure(BufferInt.constant(value = statistic.constantValue.get, length = numElems))

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
final case class SegmentLong(
    sf: Option[SharedFile],
    numElems: Int,
    statistic: StatisticLong
) extends Segment {

  def nonMissingMinMax= statistic.nonMissingMinMax

  type Elem = Long
  type BufferType = BufferLong
  type SegmentType = SegmentLong
  type ColumnType = Column.I64Column
  type ColumnTagType = ColumnTag.I64.type
  val tag = ColumnTag.I64

  override def buffer(implicit tsc: TaskSystemComponents): IO[BufferLong] = {
    sf match {
      case None if statistic.constantValue.isDefined =>
        IO.pure(BufferLong.constant(value = statistic.constantValue.get, length = numElems))
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
final case class SegmentInstant(
    sf: Option[SharedFile],
    numElems: Int,
    statistic: StatisticLong
) extends Segment {

  def nonMissingMinMax= statistic.nonMissingMinMax

  type Elem = Long
  type BufferType = BufferInstant
  type SegmentType = SegmentInstant
  type ColumnType = Column.InstantColumn
  type ColumnTagType = ColumnTag.Instant.type
  val tag = ColumnTag.Instant

  override def buffer(implicit tsc: TaskSystemComponents): IO[BufferType] = {
    sf match {
     case None if statistic.constantValue.isDefined =>
        IO.pure(BufferInstant.constant(value = statistic.constantValue.get, length = numElems))
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
final case class SegmentString(
    sf: Option[SharedFile],
    numElems: Int,
    statistic: StatisticCharSequence
) extends Segment {


  def nonMissingMinMax= statistic.nonMissingMinMax

  type Elem = CharSequence
  type BufferType = BufferString
  type SegmentType = SegmentString
  type ColumnType = Column.StringColumn
  type ColumnTagType = ColumnTag.StringTag.type
  val tag = ColumnTag.StringTag

  override def buffer(implicit tsc: TaskSystemComponents): IO[BufferType] = {
    sf match {
      case None if statistic.constantValue.isDefined =>
        IO.pure(BufferString.constant(value = statistic.constantValue.get.toString, length = numElems))
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
