package ra3

import tasks.TaskSystemComponents
import cats.effect.IO
import bufferimpl.CharSequenceOrdering

sealed trait ColumnTag { self =>
  type ColumnTagType >: this.type <: ColumnTag
  type SegmentPairType <: SegmentPair {
    type SegmentType = self.SegmentType
    type BufferType = self.BufferType
    type Elem = self.Elem
  }
  type SegmentType <: Segment {
    type ColumnTagType = self.ColumnTagType
    type SegmentType = self.SegmentType
    type BufferType = self.BufferType
    type Elem = self.Elem
  }
  type BufferType <: Buffer {
    type SegmentType = self.SegmentType
    type BufferType = self.BufferType
    type Elem = self.Elem
  }
  type Elem
  type ColumnType <: Column {
    type ColumnTagType = self.ColumnTagType
    type SegmentType = self.SegmentType
    type BufferType = self.BufferType
    type Elem = self.Elem
  }

  def makeBuffer(elems: Array[Elem]): BufferType
  def makeBufferFromSeq(elems: Elem*): BufferType // BufferDouble(elems)
  def broadcastBuffer(elem: Elem, size: Int): BufferType
  def makeColumn(segments: Vector[SegmentType]): ColumnType
  def makeColumnFromSeq(name: String, colIdx: Int)(
      elems: Seq[Seq[Elem]]
  )(implicit tsc: TaskSystemComponents): IO[ColumnType]
  def ordering: Ordering[Elem]
  def pair(a: SegmentType, b: SegmentType): SegmentPairType
  def emptySegment: SegmentType
  def cat(buffs: BufferType*): BufferType
}
object ColumnTag {
  object I32 extends ColumnTag {
    override def toString = "I32"

    def cat(buffs: BufferType*): BufferType = {
      BufferInt(org.saddle.array.flatten(buffs.map(_.values)))
    }
    type Elem = Int
    type BufferType = BufferInt
    type ColumnTagType = I32.type
    type ColumnType = Column.Int32Column
    type SegmentType = SegmentInt
    type SegmentPairType = I32Pair
    def makeBuffer(elems: Array[Int]): BufferType = BufferInt(elems)
    def makeBufferFromSeq(elems: Elem*): BufferType = BufferInt(elems.toArray)

    def broadcastBuffer(elem: Elem, size: Int): BufferType =
      BufferInt.constant(elem, size)

    def makeColumn(segments: Vector[SegmentType]): ColumnType =
      Column.Int32Column(segments)

    def makeColumnFromSeq(name: String, colIdx: Int)(
        elems: Seq[Seq[Elem]]
    )(implicit tsc: TaskSystemComponents): IO[ColumnType] =
      IO.parSequenceN(32)(elems.zipWithIndex.map { case (s, idx) =>
        this
          .makeBufferFromSeq(s: _*)
          .toSegment(LogicalPath(name, None, idx, colIdx))
      }).map(_.toVector)
        .map(this.makeColumn)

    val ordering = implicitly[Ordering[Elem]]
    def pair(a: SegmentType, b: SegmentType): SegmentPairType = I32Pair(a, b)
    val emptySegment: SegmentType = SegmentInt(None, 0, None)

  }
  object I64 extends ColumnTag {
    override def toString = "I64"

    def cat(buffs: BufferType*): BufferType = {
      BufferLong(org.saddle.array.flatten(buffs.map(_.values)))
    }
    def broadcastBuffer(elem: Elem, size: Int): BufferType = BufferLong(
      Array.fill[Long](size)(elem)
    )
    type Elem = Long
    type BufferType = BufferLong
    type ColumnTagType = I64.type
    type ColumnType = Column.I64Column
    type SegmentType = SegmentLong
    type SegmentPairType = I64Pair
    def makeBuffer(elems: Array[Long]): BufferType = BufferLong(elems)
    def makeBufferFromSeq(elems: Elem*): BufferType = BufferLong(elems.toArray)
    def makeColumn(segments: Vector[SegmentType]): ColumnType =
      Column.I64Column(segments)

    def makeColumnFromSeq(name: String, colIdx: Int)(
        elems: Seq[Seq[Elem]]
    )(implicit tsc: TaskSystemComponents): IO[ColumnType] =
      IO.parSequenceN(32)(elems.zipWithIndex.map { case (s, idx) =>
        this
          .makeBufferFromSeq(s: _*)
          .toSegment(LogicalPath(name, None, idx, colIdx))
      }).map(_.toVector)
        .map(this.makeColumn)

    val ordering = implicitly[Ordering[Elem]]
    def pair(a: SegmentType, b: SegmentType): SegmentPairType = I64Pair(a, b)
    val emptySegment: SegmentType = SegmentLong(None, 0, None)

  }
  object Instant extends ColumnTag {
    override def toString = "Instant"

    def broadcastBuffer(elem: Elem, size: Int): BufferType = BufferInstant(
      Array.fill[Long](size)(elem)
    )

    def cat(buffs: BufferType*): BufferType = {
      BufferInstant(org.saddle.array.flatten(buffs.map(_.values)))
    }
    type Elem = Long
    type BufferType = BufferInstant
    type ColumnTagType = Instant.type
    type ColumnType = Column.InstantColumn
    type SegmentType = SegmentInstant
    type SegmentPairType = InstantPair
    def makeBuffer(elems: Array[Long]): BufferType = BufferInstant(elems)
    def makeBufferFromSeq(elems: Elem*): BufferType = BufferInstant(
      elems.toArray
    )
    def makeColumn(segments: Vector[SegmentType]): ColumnType =
      Column.InstantColumn(segments)

    def makeColumnFromSeq(name: String, colIdx: Int)(
        elems: Seq[Seq[Elem]]
    )(implicit tsc: TaskSystemComponents): IO[ColumnType] =
      IO.parSequenceN(32)(elems.zipWithIndex.map { case (s, idx) =>
        this
          .makeBufferFromSeq(s: _*)
          .toSegment(LogicalPath(name, None, idx, colIdx))
      }).map(_.toVector)
        .map(this.makeColumn)

    val ordering = implicitly[Ordering[Elem]]
    def pair(a: SegmentType, b: SegmentType): SegmentPairType =
      InstantPair(a, b)
    val emptySegment: SegmentType = SegmentInstant(None, 0, None)

  }
  object StringTag extends ColumnTag {

    def broadcastBuffer(elem: Elem, size: Int): BufferType = BufferString(
      Array.fill[CharSequence](size)(elem)
    )

    def cat(buffs: BufferType*): BufferType = {
      BufferString(org.saddle.array.flatten(buffs.map(_.values)))
    }
    override def toString = "StringTag"
    type Elem = CharSequence
    type BufferType = BufferString
    type ColumnTagType = StringTag.type
    type ColumnType = Column.StringColumn
    type SegmentType = SegmentString
    type SegmentPairType = StringPair
    def makeBuffer(elems: Array[CharSequence]): BufferType = BufferString(elems)
    def makeBufferFromSeq(elems: Elem*): BufferType = BufferString(
      elems.toArray
    )
    def makeColumn(segments: Vector[SegmentType]): ColumnType =
      Column.StringColumn(segments)

    def makeColumnFromSeq(name: String, colIdx: Int)(
        elems: Seq[Seq[Elem]]
    )(implicit tsc: TaskSystemComponents): IO[ColumnType] =
      IO.parSequenceN(32)(elems.zipWithIndex.map { case (s, idx) =>
        this
          .makeBufferFromSeq(s: _*)
          .toSegment(LogicalPath(name, None, idx, colIdx))
      }).map(_.toVector)
        .map(this.makeColumn)

    val ordering = CharSequenceOrdering
    def pair(a: SegmentType, b: SegmentType): SegmentPairType = StringPair(a, b)
    val emptySegment: SegmentType = SegmentString(None, 0, None)

  }
  object F64 extends ColumnTag {
    override def toString = "F64"
    def broadcastBuffer(elem: Elem, size: Int): BufferType = BufferDouble(
      Array.fill[Double](size)(elem)
    )
    def cat(buffs: BufferType*): BufferType = {
      BufferDouble(org.saddle.array.flatten(buffs.map(_.values)))
    }
    type Elem = Double
    type BufferType = BufferDouble
    type ColumnTagType = F64.type
    type ColumnType = Column.F64Column
    type SegmentType = SegmentDouble
    type SegmentPairType = F64Pair
    def makeBuffer(elems: Array[Double]): BufferType =
      BufferDouble(elems)
    def makeBufferFromSeq(elems: Double*): BufferType =
      BufferDouble(elems.toArray)
    def makeColumn(segments: Vector[SegmentType]): ColumnType =
      Column.F64Column(segments)
    val ordering = implicitly[Ordering[Elem]]
    def pair(a: SegmentType, b: SegmentType): SegmentPairType = F64Pair(a, b)
    def makeColumnFromSeq(name: String, colIdx: Int)(
        elems: Seq[Seq[Elem]]
    )(implicit tsc: TaskSystemComponents): IO[ColumnType] =
      IO.parSequenceN(32)(elems.zipWithIndex.map { case (s, idx) =>
        this
          .makeBufferFromSeq(s: _*)
          .toSegment(LogicalPath(name, None, idx, colIdx))
      }).map(_.toVector)
        .map(this.makeColumn)

    val emptySegment: SegmentType = SegmentDouble(None, 0, None)
  }
}

trait ColumnTags {
  implicit val i32: ColumnTag.I32.type = ColumnTag.I32
  implicit val i64: ColumnTag.I64.type = ColumnTag.I64
  implicit val f64: ColumnTag.F64.type = ColumnTag.F64
  implicit val instant: ColumnTag.Instant.type = ColumnTag.Instant
}
