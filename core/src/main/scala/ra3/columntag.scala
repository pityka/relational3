package ra3

import tasks.TaskSystemComponents
import cats.effect.IO
import bufferimpl.CharSequenceOrdering
import bufferimpl.ArrayUtil

sealed trait ColumnTag { self =>
  type Elem

  def wrap(s:Seq[SegmentType]) : Box[?]

  def makeNamedColumnSpecFromBuffer(
      buffer: BufferType,
      name: String
  ): ra3.lang.NamedColumnSpecWithColumnChunkValue["", Box[
    Either[BufferType, Seq[SegmentType]]
  ]]
  def makeNamedColumnSpecFromSegments(
      segments: Seq[SegmentType],
      name: String
  ): ra3.lang.NamedColumnSpecWithColumnChunkValue["", Box[
    Either[BufferType, Seq[SegmentType]]
  ]]

  type SegmentType <: Segment {
    type Elem = self.Elem
  }
  type BufferType <: Buffer {
    type Elem = self.Elem
  }
  type ColumnType <: Column {
    type Elem = self.Elem
  }
  type TaggedBufferType <: TaggedBuffer {
    type Elem = self.Elem
  }
  type TaggedBuffersType <: TaggedBuffers {
    type Elem = self.Elem
  }
  type TaggedSegmentType <: TaggedSegment {
    type Elem = self.Elem
  }
  type TaggedSegmentsType <: TaggedSegments {
    type Elem = self.Elem
  }
  def numElems(column: ColumnType) = segments(column).map(_.numElems).sum
  def numElems(segment: SegmentType) = segment.numElems
  def numBytes(column: ColumnType) = segments(column).map(_.numBytes).sum

  def makeTaggedColumn(first: ColumnType): TaggedColumn
  def makeTaggedSegment(first: SegmentType): TaggedSegmentType
  def makeTaggedSegments(first: Seq[SegmentType]): TaggedSegmentsType

  def castAndConcatenate(first: ColumnType, other: ColumnType) =
    ++(first, other)

  def ++(first: ColumnType, other: ColumnType): ColumnType
  def segments(column: ColumnType): Vector[SegmentType]
  def segments(column: TaggedSegmentsType): Vector[SegmentType]
  def nonMissingMinMax(segment: BufferType): Option[(Elem, Elem)]
  def nonMissingMinMax(segment: SegmentType): Option[(Elem, Elem)]
  def nonMissingMinMax(segment: ColumnType): Option[(Elem, Elem)]
  def buffer(bs: TaggedBufferType): BufferType
  def makeTaggedBuffers(bs: Seq[BufferType]): TaggedBuffersType
  def makeTaggedBuffer(b: BufferType): TaggedBufferType

  def buffer(segment: SegmentType)(implicit
      tsc: TaskSystemComponents
  ): IO[BufferType]

  def toSegment(buffer: BufferType, name: LogicalPath)(implicit
      tsc: TaskSystemComponents
  ): IO[SegmentType]

  /** Returns locations at which this is <= or >= than the first element of
    * other
    */
  def findInequalityVsHead(
      one: BufferType,
      other: BufferType,
      lessThan: Boolean
  ): BufferInt

  /** If this buffer has size numElems then return it If this buffer has size 1
    * then return a buffer with that element repeated numElem times Otherwise
    * throw
    *
    * @param numElems
    */
  def broadcast(buffer: BufferType, numElems: Int): BufferType

  /** Reduce the groups by taking the first element per group */
  def firstInGroup(
      buffer: BufferType,
      partitionMap: BufferInt,
      numGroups: Int
  ): BufferType

  /** partitions the buffer with the partition map supplied Returns as many new
    * buffers of various sizes as many distinct values in the map
    */
  def partition(
      buffer: BufferType,
      numPartitions: Int,
      map: BufferInt
  ): Vector[BufferType]

  /** takes element where mask is non missing and > 0, for string mask where non
    * missing and non empty
    */
  def filter(buffer: BufferType, mask: Buffer): BufferType = {
    assert(buffer.length == mask.length, "mask length incorrect")
    take(buffer, mask.positiveLocations)
  }

  /** Takes an other buffer of the same size and returns a buffer of the same
    * size with elements from this buffer, except if it is missing then from the
    * other buffer, else missing value
    */
  def mergeNonMissing(
      first: BufferType,
      other: BufferType
  ): BufferType

  /** Computes inner, full outer, left outer or right outer joins Returns two
    * index buffers with the locations which have to be taken from the original
    * buffers (this and other) to join them. If the return is None, then take
    * the complete buffer.
    *
    * Missing values match other missing values (which is not correct)
    *
    * @param other
    * @param how
    *   one of inner outer left right
    * @return
    */
  def computeJoinIndexes(
      first: BufferType,
      other: BufferType,
      how: String
  ): (Option[BufferInt], Option[BufferInt])

  /** uses the first element from cutoff */
  def filterInEquality(
      first: BufferType,
      tag: ColumnTag,
      less: Boolean
  )(comparison: tag.BufferType, cutoff: tag.BufferType): BufferType = {
    val idx = tag.findInequalityVsHead(comparison, cutoff, less)

    this.take(first, idx)
  }

  /** negative indices yield NA values */
  def take(buffer: BufferType, locs: Location): BufferType

  /** Returns CDF of this buffer at numPoints evenly spaced points always
    * including the min and the max
    */
  def cdf(buffer: BufferType, numPoints: Int): (BufferType, BufferDouble)

  def makeBuffer(elems: Array[Elem]): BufferType
  def makeBufferFromSeq(elems: Elem*): BufferType // BufferDouble(elems)
  def broadcastBuffer(elem: Elem, size: Int): BufferType
  def makeColumn(segments: Vector[SegmentType]): ColumnType
  def makeColumnFromSeq(name: String, colIdx: Int)(
      elems: Seq[Seq[Elem]]
  )(implicit tsc: TaskSystemComponents): IO[ColumnType]
  def ordering: Ordering[Elem]
  def emptySegment: SegmentType
  def cat(buffs: BufferType*): BufferType
}
object ColumnTag {
  object I32 extends ColumnTag {
    override def toString = "I32"
    def wrap(s: Seq[SegmentType]): Box[?] = I32Var(Right(s))

    def makeNamedColumnSpecFromBuffer(
        buffer: BufferType,
        name: String
    ) = ra3.lang.NamedColumnChunkI32(I32Var(Left(buffer)), name)

    def makeNamedColumnSpecFromSegments(
        segments: Seq[SegmentType],
        name: String
    ) = ra3.lang.NamedColumnChunkI32(I32Var(Right(segments)), name)

    def broadcast(
        buffer: ra3.ColumnTag.I32.BufferType,
        numElems: Int
    ): ra3.ColumnTag.I32.BufferType = buffer match {
      case b: BufferIntConstant => b.broadcast(numElems)
      case b: BufferIntInArray  => b.broadcast(numElems)
    }

    def buffer(
        bs: ra3.ColumnTag.I32.TaggedBufferType
    ): ra3.ColumnTag.I32.BufferType = bs.buffer
    def buffer(segment: ra3.ColumnTag.I32.SegmentType)(implicit
        tsc: tasks.TaskSystemComponents
    ): cats.effect.IO[ra3.ColumnTag.I32.BufferType] = segment.buffer
    def computeJoinIndexes(
        first: ra3.ColumnTag.I32.BufferType,
        other: ra3.ColumnTag.I32.BufferType,
        how: String
    ): (Option[ra3.BufferInt], Option[ra3.BufferInt]) = first match {
      case b1: BufferIntConstant => b1.computeJoinIndexes(other, how)
      case b1: BufferIntInArray  => b1.computeJoinIndexes(other, how)
    }

    def findInequalityVsHead(
        one: ra3.ColumnTag.I32.BufferType,
        other: ra3.ColumnTag.I32.BufferType,
        lessThan: Boolean
    ): ra3.BufferInt = one match {
      case b1: BufferIntConstant => b1.findInequalityVsHead(other, lessThan)
      case b1: BufferIntInArray  => b1.findInequalityVsHead(other, lessThan)
    }
    def firstInGroup(
        buffer: ra3.ColumnTag.I32.BufferType,
        partitionMap: ra3.BufferInt,
        numGroups: Int
    ): ra3.ColumnTag.I32.BufferType = buffer match {
      case b1: BufferIntConstant => b1.firstInGroup(partitionMap, numGroups)
      case b1: BufferIntInArray  => b1.firstInGroup(partitionMap, numGroups)
    }

    def makeTaggedBuffer(
        b: ra3.ColumnTag.I32.BufferType
    ): ra3.ColumnTag.I32.TaggedBufferType = b
    def makeTaggedBuffers(
        bs: Seq[ra3.ColumnTag.I32.BufferType]
    ): ra3.ColumnTag.I32.TaggedBuffersType = TaggedBuffersI32(bs)
    def makeTaggedColumn(
        first: ra3.ColumnTag.I32.ColumnType
    ): ra3.TaggedColumn = {
      assert(first.segments.forall(_.isInstanceOf[SegmentInt]))
      TaggedColumn.TaggedColumnI32(first)
    }
    def makeTaggedSegment(
        first: ra3.ColumnTag.I32.SegmentType
    ): ra3.ColumnTag.I32.TaggedSegmentType = first
    def makeTaggedSegments(
        first: Seq[ra3.ColumnTag.I32.SegmentType]
    ): ra3.ColumnTag.I32.TaggedSegmentsType = TaggedSegmentsI32(first)
    def mergeNonMissing(
        first: ra3.ColumnTag.I32.BufferType,
        other: ra3.ColumnTag.I32.BufferType
    ): ra3.ColumnTag.I32.BufferType = first match {
      case b1: BufferIntConstant => b1.mergeNonMissing(other)
      case b1: BufferIntInArray  => b1.mergeNonMissing(other)
    }

    def nonMissingMinMax(
        segment: ra3.ColumnTag.I32.BufferType
    ): Option[(ra3.ColumnTag.I32.Elem, ra3.ColumnTag.I32.Elem)] =
      segment.nonMissingMinMax
    def nonMissingMinMax(
        segment: ra3.ColumnTag.I32.SegmentType
    ): Option[(ra3.ColumnTag.I32.Elem, ra3.ColumnTag.I32.Elem)] =
      segment.nonMissingMinMax
    def nonMissingMinMax(
        segment: ra3.ColumnTag.I32.ColumnType
    ): Option[(ra3.ColumnTag.I32.Elem, ra3.ColumnTag.I32.Elem)] =
      segment.nonMissingMinMax
    def partition(
        buffer: ra3.ColumnTag.I32.BufferType,
        numPartitions: Int,
        map: ra3.BufferInt
    ): Vector[ra3.ColumnTag.I32.BufferType] = buffer match {
      case b1: BufferIntConstant => b1.partition(numPartitions, map)
      case b1: BufferIntInArray  => b1.partition(numPartitions, map)
    }

    def segments(
        column: ra3.ColumnTag.I32.ColumnType
    ): Vector[ra3.ColumnTag.I32.SegmentType] = column.segments
    def segments(
        column: ra3.ColumnTag.I32.TaggedSegmentsType
    ): Vector[ra3.ColumnTag.I32.SegmentType] = column.segments.toVector
    def take(
        buffer: ra3.ColumnTag.I32.BufferType,
        locs: ra3.Location
    ): ra3.ColumnTag.I32.BufferType = buffer match {
      case b1: BufferIntConstant => b1.take(locs)
      case b1: BufferIntInArray  => b1.take(locs)
    }

    def toSegment(buffer: ra3.ColumnTag.I32.BufferType, name: ra3.LogicalPath)(
        implicit tsc: tasks.TaskSystemComponents
    ): cats.effect.IO[ra3.ColumnTag.I32.SegmentType] = buffer match {
      case b1: BufferIntConstant => b1.toSegment(name)
      case b1: BufferIntInArray  => b1.toSegment(name)
    }

    def ++(
        first: Column.Int32Column,
        other: Column.Int32Column
    ): Column.Int32Column = Column.Int32Column(
      first.segments ++ other.segments
    )

    def cdf(buffer: BufferType, numPoints: Int): (BufferInt, BufferDouble) = {
      val percentiles =
        ((0 until (numPoints - 1)).map(i => i * (1d / (numPoints - 1))) ++ List(
          1d
        )).distinct

      val sorted: Array[Int] = {
        val cpy = ra3.bufferimpl.ArrayUtil.dropNAI(buffer.values)
        java.util.Arrays.sort(cpy)
        cpy
      }
      val cdf = percentiles.map { p =>
        val idx = (p * (sorted.length - 1)).toInt
        (sorted(idx), p)
      }

      val x = BufferInt(cdf.map(_._1).toArray)
      val y = BufferDouble(cdf.map(_._2).toArray)
      (x, y)
    }

    def cat(buffs: BufferType*): BufferType = {
      assert(buffs.forall(_.isInstanceOf[BufferInt]))
      BufferInt(ArrayUtil.flattenI(buffs.map(_.values)))
    }
    type Elem = Int
    type BufferType = BufferInt
    type ColumnType = Column.Int32Column
    type SegmentType = SegmentInt
    type TaggedBufferType = BufferInt
    type TaggedBuffersType = TaggedBuffersI32
    type TaggedSegmentType = SegmentInt
    type TaggedSegmentsType = TaggedSegmentsI32
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
        this.toSegment(
          this
            .makeBufferFromSeq(s*),
          LogicalPath(name, None, idx, colIdx)
        )
      }).map(_.toVector)
        .map(this.makeColumn)

    val ordering = implicitly[Ordering[Elem]]
    val emptySegment: SegmentType = SegmentInt(None, 0, StatisticInt.empty)

  }
  object I64 extends ColumnTag {
    override def toString = "I64"

        def wrap(s: Seq[SegmentType]): Box[?] = I64Var(Right(s))


    def makeNamedColumnSpecFromBuffer(
        buffer: BufferType,
        name: String
    ) = ra3.lang.NamedColumnChunkI64(I64Var(Left(buffer)), name)

    def makeNamedColumnSpecFromSegments(
        segments: Seq[SegmentType],
        name: String
    ) = ra3.lang.NamedColumnChunkI64(I64Var(Right(segments)), name)

    def ++(
        first: ra3.ColumnTag.I64.ColumnType,
        other: ra3.ColumnTag.I64.ColumnType
    ): ra3.ColumnTag.I64.ColumnType = first.++(other)
    def broadcast(
        buffer: ra3.ColumnTag.I64.BufferType,
        numElems: Int
    ): ra3.ColumnTag.I64.BufferType = buffer.broadcast(numElems)
    def buffer(
        bs: ra3.ColumnTag.I64.TaggedBufferType
    ): ra3.ColumnTag.I64.BufferType = bs.buffer
    def buffer(segment: ra3.ColumnTag.I64.SegmentType)(implicit
        tsc: tasks.TaskSystemComponents
    ): cats.effect.IO[ra3.ColumnTag.I64.BufferType] = segment.buffer
    def computeJoinIndexes(
        first: ra3.ColumnTag.I64.BufferType,
        other: ra3.ColumnTag.I64.BufferType,
        how: String
    ): (Option[ra3.BufferInt], Option[ra3.BufferInt]) =
      first.computeJoinIndexes(other, how)
    def findInequalityVsHead(
        one: ra3.ColumnTag.I64.BufferType,
        other: ra3.ColumnTag.I64.BufferType,
        lessThan: Boolean
    ): ra3.BufferInt = one.findInequalityVsHead(other, lessThan)
    def firstInGroup(
        buffer: ra3.ColumnTag.I64.BufferType,
        partitionMap: ra3.BufferInt,
        numGroups: Int
    ): ra3.ColumnTag.I64.BufferType =
      buffer.firstInGroup(partitionMap, numGroups)
    def makeTaggedBuffer(
        b: ra3.ColumnTag.I64.BufferType
    ): ra3.ColumnTag.I64.TaggedBufferType = b
    def makeTaggedBuffers(
        bs: Seq[ra3.ColumnTag.I64.BufferType]
    ): ra3.ColumnTag.I64.TaggedBuffersType = TaggedBuffersI64(bs)
    def makeTaggedColumn(
        first: ra3.ColumnTag.I64.ColumnType
    ): ra3.TaggedColumn = {
      assert(first.segments.forall(_.isInstanceOf[SegmentLong]))
      TaggedColumn.TaggedColumnI64(first)
    }
    def makeTaggedSegment(
        first: ra3.ColumnTag.I64.SegmentType
    ): ra3.ColumnTag.I64.TaggedSegmentType = first
    def makeTaggedSegments(
        first: Seq[ra3.ColumnTag.I64.SegmentType]
    ): ra3.ColumnTag.I64.TaggedSegmentsType = TaggedSegmentsI64(first)
    def mergeNonMissing(
        first: ra3.ColumnTag.I64.BufferType,
        other: ra3.ColumnTag.I64.BufferType
    ): ra3.ColumnTag.I64.BufferType = first.mergeNonMissing(other)
    def nonMissingMinMax(
        segment: ra3.ColumnTag.I64.BufferType
    ): Option[(ra3.ColumnTag.I64.Elem, ra3.ColumnTag.I64.Elem)] =
      segment.nonMissingMinMax
    def nonMissingMinMax(
        segment: ra3.ColumnTag.I64.SegmentType
    ): Option[(ra3.ColumnTag.I64.Elem, ra3.ColumnTag.I64.Elem)] =
      segment.nonMissingMinMax
    def nonMissingMinMax(
        segment: ra3.ColumnTag.I64.ColumnType
    ): Option[(ra3.ColumnTag.I64.Elem, ra3.ColumnTag.I64.Elem)] =
      segment.nonMissingMinMax
    def partition(
        buffer: ra3.ColumnTag.I64.BufferType,
        numPartitions: Int,
        map: ra3.BufferInt
    ): Vector[ra3.ColumnTag.I64.BufferType] =
      buffer.partition(numPartitions, map)
    def segments(
        column: ra3.ColumnTag.I64.ColumnType
    ): Vector[ra3.ColumnTag.I64.SegmentType] = column.segments
    def segments(
        column: ra3.ColumnTag.I64.TaggedSegmentsType
    ): Vector[ra3.ColumnTag.I64.SegmentType] = column.segments.toVector
    def take(
        buffer: ra3.ColumnTag.I64.BufferType,
        locs: ra3.Location
    ): ra3.ColumnTag.I64.BufferType = buffer.take(locs)
    def toSegment(buffer: ra3.ColumnTag.I64.BufferType, name: ra3.LogicalPath)(
        implicit tsc: tasks.TaskSystemComponents
    ): cats.effect.IO[ra3.ColumnTag.I64.SegmentType] = buffer.toSegment(name)
    def cdf(buffer: BufferType, numPoints: Int): (BufferLong, BufferDouble) = {
      val percentiles =
        ((0 until (numPoints - 1)).map(i => i * (1d / (numPoints - 1))) ++ List(
          1d
        )).distinct
      val sorted: Array[Long] = {
        val cpy = ra3.bufferimpl.ArrayUtil.dropNAL(buffer.values)
        java.util.Arrays.sort(cpy)
        cpy
      }
      val cdf = percentiles.map { p =>
        val idx = (p * (sorted.length - 1)).toInt
        (sorted(idx), p)
      }

      val x = BufferLong(cdf.map(_._1).toArray)
      val y = BufferDouble(cdf.map(_._2).toArray)
      (x, y)
    }

    def cat(buffs: BufferType*): BufferType = {
      assert(buffs.forall(_.isInstanceOf[BufferLong]))
      BufferLong(ArrayUtil.flattenL(buffs.map(_.values)))
    }
    def broadcastBuffer(elem: Elem, size: Int): BufferType = BufferLong(
      Array.fill[Long](size)(elem)
    )
    type Elem = Long
    type BufferType = BufferLong
    type ColumnType = Column.I64Column
    type SegmentType = SegmentLong
    type TaggedBufferType = BufferLong
    type TaggedBuffersType = TaggedBuffersI64
    type TaggedSegmentType = SegmentLong
    type TaggedSegmentsType = TaggedSegmentsI64

    def makeBuffer(elems: Array[Long]): BufferType = BufferLong(elems)
    def makeBufferFromSeq(elems: Elem*): BufferType = BufferLong(elems.toArray)
    def makeColumn(segments: Vector[SegmentType]): ColumnType =
      Column.I64Column(segments)

    def makeColumnFromSeq(name: String, colIdx: Int)(
        elems: Seq[Seq[Elem]]
    )(implicit tsc: TaskSystemComponents): IO[ColumnType] =
      IO.parSequenceN(32)(elems.zipWithIndex.map { case (s, idx) =>
        this
          .makeBufferFromSeq(s*)
          .toSegment(LogicalPath(name, None, idx, colIdx))
      }).map(_.toVector)
        .map(this.makeColumn)

    val ordering = implicitly[Ordering[Elem]]
    val emptySegment: SegmentType = SegmentLong(None, 0, StatisticLong.empty)

  }
  object Instant extends ColumnTag {
    override def toString = "Instant"

        def wrap(s: Seq[SegmentType]): Box[?] = InstVar(Right(s))


    def makeNamedColumnSpecFromBuffer(
        buffer: BufferType,
        name: String
    ) = ra3.lang.NamedColumnChunkInst(InstVar(Left(buffer)), name)

    def makeNamedColumnSpecFromSegments(
        segments: Seq[SegmentType],
        name: String
    ) = ra3.lang.NamedColumnChunkInst(InstVar(Right(segments)), name)

    def ++(
        first: ra3.ColumnTag.Instant.ColumnType,
        other: ra3.ColumnTag.Instant.ColumnType
    ): ra3.ColumnTag.Instant.ColumnType = first ++ other
    def broadcast(
        buffer: ra3.ColumnTag.Instant.BufferType,
        numElems: Int
    ): ra3.ColumnTag.Instant.BufferType = buffer.broadcast(numElems)
    def buffer(
        bs: ra3.ColumnTag.Instant.TaggedBufferType
    ): ra3.ColumnTag.Instant.BufferType = bs.buffer
    def buffer(segment: ra3.ColumnTag.Instant.SegmentType)(implicit
        tsc: tasks.TaskSystemComponents
    ): cats.effect.IO[ra3.ColumnTag.Instant.BufferType] = segment.buffer
    def computeJoinIndexes(
        first: ra3.ColumnTag.Instant.BufferType,
        other: ra3.ColumnTag.Instant.BufferType,
        how: String
    ): (Option[ra3.BufferInt], Option[ra3.BufferInt]) =
      first.computeJoinIndexes(other, how)
    def findInequalityVsHead(
        one: ra3.ColumnTag.Instant.BufferType,
        other: ra3.ColumnTag.Instant.BufferType,
        lessThan: Boolean
    ): ra3.BufferInt = one.findInequalityVsHead(other, lessThan)
    def firstInGroup(
        buffer: ra3.ColumnTag.Instant.BufferType,
        partitionMap: ra3.BufferInt,
        numGroups: Int
    ): ra3.ColumnTag.Instant.BufferType =
      buffer.firstInGroup(partitionMap, numGroups)
    def makeTaggedBuffer(
        b: ra3.ColumnTag.Instant.BufferType
    ): ra3.ColumnTag.Instant.TaggedBufferType = b
    def makeTaggedBuffers(
        bs: Seq[ra3.ColumnTag.Instant.BufferType]
    ): ra3.ColumnTag.Instant.TaggedBuffersType = TaggedBuffersInstant(bs)
    def makeTaggedColumn(
        first: ra3.ColumnTag.Instant.ColumnType
    ): ra3.TaggedColumn = {
      assert(first.segments.forall(_.isInstanceOf[SegmentInstant]))
      TaggedColumn.TaggedColumnInstant(first)
    }
    def makeTaggedSegment(
        first: ra3.ColumnTag.Instant.SegmentType
    ): ra3.ColumnTag.Instant.TaggedSegmentType = first
    def makeTaggedSegments(
        first: Seq[ra3.ColumnTag.Instant.SegmentType]
    ): ra3.ColumnTag.Instant.TaggedSegmentsType = TaggedSegmentsInstant(first)
    def mergeNonMissing(
        first: ra3.ColumnTag.Instant.BufferType,
        other: ra3.ColumnTag.Instant.BufferType
    ): ra3.ColumnTag.Instant.BufferType = first.mergeNonMissing(other)
    def nonMissingMinMax(
        segment: ra3.ColumnTag.Instant.BufferType
    ): Option[(ra3.ColumnTag.Instant.Elem, ra3.ColumnTag.Instant.Elem)] =
      segment.nonMissingMinMax
    def nonMissingMinMax(
        segment: ra3.ColumnTag.Instant.SegmentType
    ): Option[(ra3.ColumnTag.Instant.Elem, ra3.ColumnTag.Instant.Elem)] =
      segment.nonMissingMinMax
    def nonMissingMinMax(
        segment: ra3.ColumnTag.Instant.ColumnType
    ): Option[(ra3.ColumnTag.Instant.Elem, ra3.ColumnTag.Instant.Elem)] =
      segment.nonMissingMinMax
    def partition(
        buffer: ra3.ColumnTag.Instant.BufferType,
        numPartitions: Int,
        map: ra3.BufferInt
    ): Vector[ra3.ColumnTag.Instant.BufferType] =
      buffer.partition(numPartitions, map)
    def segments(
        column: ra3.ColumnTag.Instant.ColumnType
    ): Vector[ra3.ColumnTag.Instant.SegmentType] = column.segments
    def segments(
        column: ra3.ColumnTag.Instant.TaggedSegmentsType
    ): Vector[ra3.ColumnTag.Instant.SegmentType] = column.segments.toVector
    def take(
        buffer: ra3.ColumnTag.Instant.BufferType,
        locs: ra3.Location
    ): ra3.ColumnTag.Instant.BufferType = buffer.take(locs)
    def toSegment(
        buffer: ra3.ColumnTag.Instant.BufferType,
        name: ra3.LogicalPath
    )(implicit
        tsc: tasks.TaskSystemComponents
    ): cats.effect.IO[ra3.ColumnTag.Instant.SegmentType] =
      buffer.toSegment(name)

    def cdf(
        buffer: BufferType,
        numPoints: Int
    ): (BufferInstant, BufferDouble) = {
      val percentiles =
        ((0 until (numPoints - 1)).map(i => i * (1d / (numPoints - 1))) ++ List(
          1d
        )).distinct
      val sorted: Array[Long] = {
        val cpy = ra3.bufferimpl.ArrayUtil.dropNAL(buffer.values)
        java.util.Arrays.sort(cpy)
        cpy
      }
      val cdf = percentiles.map { p =>
        val idx = (p * (sorted.length - 1)).toInt
        (sorted(idx), p)
      }

      val x = BufferInstant(cdf.map(_._1).toArray)
      val y = BufferDouble(cdf.map(_._2).toArray)
      (x, y)
    }
    def broadcastBuffer(elem: Elem, size: Int): BufferType = BufferInstant(
      Array.fill[Long](size)(elem)
    )

    def cat(buffs: BufferType*): BufferType = {
      assert(buffs.forall(_.isInstanceOf[BufferInstant]))
      BufferInstant(ArrayUtil.flattenL(buffs.map(_.values)))
    }
    type Elem = Long
    type BufferType = BufferInstant
    type ColumnType = Column.InstantColumn
    type SegmentType = SegmentInstant

    type TaggedBufferType = BufferInstant
    type TaggedBuffersType = TaggedBuffersInstant
    type TaggedSegmentType = SegmentInstant
    type TaggedSegmentsType = TaggedSegmentsInstant
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
          .makeBufferFromSeq(s*)
          .toSegment(LogicalPath(name, None, idx, colIdx))
      }).map(_.toVector)
        .map(this.makeColumn)

    val ordering = implicitly[Ordering[Elem]]
    val emptySegment: SegmentType = SegmentInstant(None, 0, StatisticLong.empty)

  }
  object StringTag extends ColumnTag {

        def wrap(s: Seq[SegmentType]): Box[?] = StrVar(Right(s))


    def makeNamedColumnSpecFromBuffer(
        buffer: BufferType,
        name: String
    ) = ra3.lang.NamedColumnChunkStr(StrVar(Left(buffer)), name)

    def makeNamedColumnSpecFromSegments(
        segments: Seq[SegmentType],
        name: String
    ) = ra3.lang.NamedColumnChunkStr(StrVar(Right(segments)), name)

    def ++(
        first: ra3.ColumnTag.StringTag.ColumnType,
        other: ra3.ColumnTag.StringTag.ColumnType
    ): ra3.ColumnTag.StringTag.ColumnType = first.++(other)
    def broadcast(
        buffer: ra3.ColumnTag.StringTag.BufferType,
        numElems: Int
    ): ra3.ColumnTag.StringTag.BufferType = buffer.broadcast(numElems)
    def buffer(
        bs: ra3.ColumnTag.StringTag.TaggedBufferType
    ): ra3.ColumnTag.StringTag.BufferType = bs.buffer
    def buffer(segment: ra3.ColumnTag.StringTag.SegmentType)(implicit
        tsc: tasks.TaskSystemComponents
    ): cats.effect.IO[ra3.ColumnTag.StringTag.BufferType] = segment.buffer
    def cdf(
        buffer: ra3.ColumnTag.StringTag.BufferType,
        numPoints: Int
    ): (ra3.ColumnTag.StringTag.BufferType, ra3.BufferDouble) =
      buffer.cdf(numPoints)
    def computeJoinIndexes(
        first: ra3.ColumnTag.StringTag.BufferType,
        other: ra3.ColumnTag.StringTag.BufferType,
        how: String
    ): (Option[ra3.BufferInt], Option[ra3.BufferInt]) =
      first.computeJoinIndexes(other, how)
    def findInequalityVsHead(
        one: ra3.ColumnTag.StringTag.BufferType,
        other: ra3.ColumnTag.StringTag.BufferType,
        lessThan: Boolean
    ): ra3.BufferInt = one.findInequalityVsHead(other, lessThan)
    def firstInGroup(
        buffer: ra3.ColumnTag.StringTag.BufferType,
        partitionMap: ra3.BufferInt,
        numGroups: Int
    ): ra3.ColumnTag.StringTag.BufferType =
      buffer.firstInGroup(partitionMap, numGroups)
    def makeTaggedBuffer(
        b: ra3.ColumnTag.StringTag.BufferType
    ): ra3.ColumnTag.StringTag.TaggedBufferType = b
    def makeTaggedBuffers(
        bs: Seq[ra3.ColumnTag.StringTag.BufferType]
    ): ra3.ColumnTag.StringTag.TaggedBuffersType = TaggedBuffersString(bs)
    def makeTaggedColumn(
        first: ra3.ColumnTag.StringTag.ColumnType
    ): ra3.TaggedColumn = {
      assert(first.segments.forall(_.isInstanceOf[SegmentString]))
      TaggedColumn.TaggedColumnString(first)
    }
    def makeTaggedSegment(
        first: ra3.ColumnTag.StringTag.SegmentType
    ): ra3.ColumnTag.StringTag.TaggedSegmentType = first
    def makeTaggedSegments(
        first: Seq[ra3.ColumnTag.StringTag.SegmentType]
    ): ra3.ColumnTag.StringTag.TaggedSegmentsType = TaggedSegmentsString(first)
    def mergeNonMissing(
        first: ra3.ColumnTag.StringTag.BufferType,
        other: ra3.ColumnTag.StringTag.BufferType
    ): ra3.ColumnTag.StringTag.BufferType = first.mergeNonMissing(other)
    def nonMissingMinMax(
        segment: ra3.ColumnTag.StringTag.BufferType
    ): Option[(ra3.ColumnTag.StringTag.Elem, ra3.ColumnTag.StringTag.Elem)] =
      segment.nonMissingMinMax
    def nonMissingMinMax(
        segment: ra3.ColumnTag.StringTag.SegmentType
    ): Option[(ra3.ColumnTag.StringTag.Elem, ra3.ColumnTag.StringTag.Elem)] =
      segment.nonMissingMinMax
    def nonMissingMinMax(
        segment: ra3.ColumnTag.StringTag.ColumnType
    ): Option[(ra3.ColumnTag.StringTag.Elem, ra3.ColumnTag.StringTag.Elem)] =
      segment.nonMissingMinMax
    def partition(
        buffer: ra3.ColumnTag.StringTag.BufferType,
        numPartitions: Int,
        map: ra3.BufferInt
    ): Vector[ra3.ColumnTag.StringTag.BufferType] =
      buffer.partition(numPartitions, map)
    def segments(
        column: ra3.ColumnTag.StringTag.ColumnType
    ): Vector[ra3.ColumnTag.StringTag.SegmentType] = column.segments
    def segments(
        column: ra3.ColumnTag.StringTag.TaggedSegmentsType
    ): Vector[ra3.ColumnTag.StringTag.SegmentType] = column.segments.toVector
    def take(
        buffer: ra3.ColumnTag.StringTag.BufferType,
        locs: ra3.Location
    ): ra3.ColumnTag.StringTag.BufferType = buffer.take(locs)
    def toSegment(
        buffer: ra3.ColumnTag.StringTag.BufferType,
        name: ra3.LogicalPath
    )(implicit
        tsc: tasks.TaskSystemComponents
    ): cats.effect.IO[ra3.ColumnTag.StringTag.SegmentType] =
      buffer.toSegment(name)

    def broadcastBuffer(elem: Elem, size: Int): BufferType = BufferString(
      Array.fill[CharSequence](size)(elem)
    )

    def cat(buffs: BufferType*): BufferType = {
      assert(buffs.forall(_.isInstanceOf[BufferString]))
      BufferString(ArrayUtil.flattenG(buffs.map(_.values)))
    }
    override def toString = "StringTag"
    type Elem = CharSequence
    type BufferType = BufferString
    type ColumnTagType = StringTag.type
    type ColumnType = Column.StringColumn
    type SegmentType = SegmentString

    type TaggedBufferType = BufferString
    type TaggedBuffersType = TaggedBuffersString
    type TaggedSegmentType = SegmentString
    type TaggedSegmentsType = TaggedSegmentsString
    def makeBuffer(elems: Array[CharSequence]): BufferType = {
      BufferString(elems)
    }
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
          .makeBufferFromSeq(s*)
          .toSegment(LogicalPath(name, None, idx, colIdx))
      }).map(_.toVector)
        .map(this.makeColumn)

    val ordering = CharSequenceOrdering
    val emptySegment: SegmentType =
      SegmentString(None, 0, 0L, StatisticCharSequence.empty)

  }
  object F64 extends ColumnTag {
    override def toString = "F64"

        def wrap(s: Seq[SegmentType]): Box[?] = F64Var(Right(s))


    def makeNamedColumnSpecFromBuffer(
        buffer: BufferType,
        name: String
    ) = ra3.lang.NamedColumnChunkF64(F64Var(Left(buffer)), name)

    def makeNamedColumnSpecFromSegments(
        segments: Seq[SegmentType],
        name: String
    ) = ra3.lang.NamedColumnChunkF64(F64Var(Right(segments)), name)

    def ++(
        first: ra3.ColumnTag.F64.ColumnType,
        other: ra3.ColumnTag.F64.ColumnType
    ): ra3.ColumnTag.F64.ColumnType = first.++(other)
    def broadcast(
        buffer: ra3.ColumnTag.F64.BufferType,
        numElems: Int
    ): ra3.ColumnTag.F64.BufferType = buffer.broadcast(numElems)
    def buffer(
        bs: ra3.ColumnTag.F64.TaggedBufferType
    ): ra3.ColumnTag.F64.BufferType = bs
    def buffer(segment: ra3.ColumnTag.F64.SegmentType)(implicit
        tsc: tasks.TaskSystemComponents
    ): cats.effect.IO[ra3.ColumnTag.F64.BufferType] = segment.buffer
    def cdf(
        buffer: ra3.ColumnTag.F64.BufferType,
        numPoints: Int
    ): (ra3.ColumnTag.F64.BufferType, ra3.BufferDouble) = buffer.cdf(numPoints)
    def findInequalityVsHead(
        one: ra3.ColumnTag.F64.BufferType,
        other: ra3.ColumnTag.F64.BufferType,
        lessThan: Boolean
    ): ra3.BufferInt = one.findInequalityVsHead(other, lessThan)
    def firstInGroup(
        buffer: ra3.ColumnTag.F64.BufferType,
        partitionMap: ra3.BufferInt,
        numGroups: Int
    ): ra3.ColumnTag.F64.BufferType =
      buffer.firstInGroup(partitionMap, numGroups)
    def makeTaggedBuffer(
        b: ra3.ColumnTag.F64.BufferType
    ): ra3.ColumnTag.F64.TaggedBufferType = b
    def makeTaggedBuffers(
        bs: Seq[ra3.ColumnTag.F64.BufferType]
    ): ra3.ColumnTag.F64.TaggedBuffersType = TaggedBuffersF64(bs)
    def makeTaggedColumn(
        first: ra3.ColumnTag.F64.ColumnType
    ): ra3.TaggedColumn = {
      assert(first.segments.forall(_.isInstanceOf[SegmentDouble]))
      TaggedColumn.TaggedColumnF64(first)
    }
    def makeTaggedSegment(
        first: ra3.ColumnTag.F64.SegmentType
    ): ra3.ColumnTag.F64.TaggedSegmentType = first
    def makeTaggedSegments(
        first: Seq[ra3.ColumnTag.F64.SegmentType]
    ): ra3.ColumnTag.F64.TaggedSegmentsType = TaggedSegmentsF64(first)
    def mergeNonMissing(
        first: ra3.ColumnTag.F64.BufferType,
        other: ra3.ColumnTag.F64.BufferType
    ): ra3.ColumnTag.F64.BufferType = first.mergeNonMissing(other)
    def nonMissingMinMax(
        segment: ra3.ColumnTag.F64.BufferType
    ): Option[(ra3.ColumnTag.F64.Elem, ra3.ColumnTag.F64.Elem)] =
      segment.nonMissingMinMax
    def nonMissingMinMax(
        segment: ra3.ColumnTag.F64.SegmentType
    ): Option[(ra3.ColumnTag.F64.Elem, ra3.ColumnTag.F64.Elem)] =
      segment.nonMissingMinMax
    def nonMissingMinMax(
        segment: ra3.ColumnTag.F64.ColumnType
    ): Option[(ra3.ColumnTag.F64.Elem, ra3.ColumnTag.F64.Elem)] =
      segment.nonMissingMinMax
    def partition(
        buffer: ra3.ColumnTag.F64.BufferType,
        numPartitions: Int,
        map: ra3.BufferInt
    ): Vector[ra3.ColumnTag.F64.BufferType] =
      buffer.partition(numPartitions, map)
    def segments(
        column: ra3.ColumnTag.F64.ColumnType
    ): Vector[ra3.ColumnTag.F64.SegmentType] = column.segments
    def segments(
        column: ra3.ColumnTag.F64.TaggedSegmentsType
    ): Vector[ra3.ColumnTag.F64.SegmentType] = column.segments.toVector
    def take(
        buffer: ra3.ColumnTag.F64.BufferType,
        locs: ra3.Location
    ): ra3.ColumnTag.F64.BufferType = buffer.take(locs)
    def toSegment(buffer: ra3.ColumnTag.F64.BufferType, name: ra3.LogicalPath)(
        implicit tsc: tasks.TaskSystemComponents
    ): cats.effect.IO[ra3.ColumnTag.F64.SegmentType] = buffer.toSegment(name)
    def broadcastBuffer(elem: Elem, size: Int): BufferType = BufferDouble(
      Array.fill[Double](size)(elem)
    )
    def cat(buffs: BufferType*): BufferType = {
      assert(buffs.forall(_.isInstanceOf[BufferDouble]))
      BufferDouble(ArrayUtil.flattenD(buffs.map(_.values)))
    }

    override def computeJoinIndexes(
        first: BufferType,
        other: BufferType,
        how: String
    ): (Option[BufferInt], Option[BufferInt]) = {
      import ra3.join.locator.LocatorDouble
      import ra3.join.*
      val idx1 = LocatorDouble.fromKeys(first.values)
      val idx2 = LocatorDouble.fromKeys(other.values)
      val reindexer = (ra3.join.JoinerImplDouble).join(
        left = idx1,
        right = idx2,
        how = how match {
          case "inner" => InnerJoin
          case "left"  => LeftJoin
          case "right" => RightJoin
          case "outer" => OuterJoin
        }
      )
      (reindexer.lTake.map(BufferInt(_)), reindexer.rTake.map(BufferInt(_)))
    }
    type Elem = Double
    type BufferType = BufferDouble
    type ColumnType = Column.F64Column
    type SegmentType = SegmentDouble

    type TaggedBufferType = BufferDouble
    type TaggedBuffersType = TaggedBuffersF64
    type TaggedSegmentType = SegmentDouble
    type TaggedSegmentsType = TaggedSegmentsF64
    def makeBuffer(elems: Array[Double]): BufferType =
      BufferDouble(elems)
    def makeBufferFromSeq(elems: Double*): BufferType =
      BufferDouble(elems.toArray)
    def makeColumn(segments: Vector[SegmentType]): ColumnType =
      Column.F64Column(segments)
    val ordering = implicitly[Ordering[Elem]]
    def makeColumnFromSeq(name: String, colIdx: Int)(
        elems: Seq[Seq[Elem]]
    )(implicit tsc: TaskSystemComponents): IO[ColumnType] =
      IO.parSequenceN(32)(elems.zipWithIndex.map { case (s, idx) =>
        this
          .makeBufferFromSeq(s*)
          .toSegment(LogicalPath(name, None, idx, colIdx))
      }).map(_.toVector)
        .map(this.makeColumn)

    val emptySegment: SegmentType =
      SegmentDouble(None, 0, StatisticDouble.empty)
  }
}
