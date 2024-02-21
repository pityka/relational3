package ra3

import cats.effect.IO
import tasks.{TaskSystemComponents}
import bufferimpl.BufferDoubleImpl
import bufferimpl.BufferInstantImpl
import bufferimpl.BufferIntConstantImpl
import bufferimpl.BufferIntImpl
import bufferimpl.BufferLongImpl
import bufferimpl.BufferStringImpl
import bufferimpl.BufferIntArrayImpl

sealed trait Location
final case class Slice(start: Int, until: Int) extends Location

sealed trait Buffer { self =>

  type Elem
  type BufferType >: this.type <: Buffer
  type SegmentType <: Segment {
    type BufferType = self.BufferType
    type SegmentType = self.SegmentType
    type Elem = self.Elem
  }
  type ColumnType <: Column {
    type Elem = self.Elem
    type BufferType = self.BufferType
    type SegmentType = self.SegmentType
  }

  type ColumnTagType <: ColumnTag {
    type ColumnType = self.ColumnType
    type SegmentType = self.SegmentType
    type BufferType = self.BufferType
    type Elem = self.Elem
  }
  def tag: ColumnTagType

  def as(b: Buffer) = this.asInstanceOf[b.BufferType]
  def as(b: ColumnTag) = this.asInstanceOf[b.BufferType]

  /** given the bounds on BufferType this should never fail */
  def asBufferType = this.asInstanceOf[BufferType]

  /** Returns the items in the buffer as a Seq
   * 
   * Missing values are encoded onto the same value as used internally
   */
  def toSeq: Seq[Elem]

  def elementAsCharSequence(i: Int): CharSequence 

  /** Returns locations at which this is <= or >= than the first element of
    * other
    */
  def findInequalityVsHead(
      other: BufferType,
      lessThan: Boolean
  ): BufferInt

  /** Returns CDF of this buffer at numPoints evenly spaced points always
    * including the min and the max
    */
  def cdf(numPoints: Int): (BufferType, BufferDouble)

  /** Returns a map of unique items */
  def groups: Buffer.GroupMap

  def length: Int

  /** negative indices yield NA values */
  def take(locs: Location): BufferType

  /** partitions the buffer with the partition map supplied 
   * Returns as many new buffers of various sizes as many distinct values in the map
   * 
  */
  def partition(numPartitions: Int, map: BufferInt) : Vector[BufferType]

  /** returns indexes where value is positive (or positive length) */
  def positiveLocations: BufferInt

  /** takes element where mask is non missing and > 0, for string mask where non
    * missing and non empty
    */
  def filter(mask: Buffer): BufferType = {
    assert(this.length == mask.length, "mask length incorrect")
    this.take(mask.positiveLocations)
  }

  /** uses the first element from cutoff */
  def filterInEquality[
      B <: Buffer { type BufferType = B }
  ](
      comparison: B,
      cutoff: B,
      less: Boolean
  ): BufferType = {
    val idx = comparison.findInequalityVsHead(cutoff.asBufferType, less)

    this.take(idx)
  }

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
      other: BufferType,
      how: String
  ): (Option[BufferInt], Option[BufferInt])

  /** Takes an other buffer of the same size and returns a buffer of the same
    * size with elements from this buffer, except if it is missing then from the
    * other buffer, else missing value
    */
  def mergeNonMissing(
      other: BufferType
  ): BufferType

  /** Returns true if item at position l is missing. Throws if out of bounds */
  def isMissing(l: Int): Boolean

  /** Returns hash of item at location l. Missing values get the same hash.
    * Throws if out of bounds
    */
  def hashOf(l: Int): Long
  def toSegment(name: LogicalPath)(implicit
      tsc: TaskSystemComponents
  ): IO[SegmentType]


  /** Reduce the groups by taking the first element per group */
  def firstInGroup(partitionMap: BufferInt, numGroups: Int): BufferType 

  /** If this buffer has size numElems then return it If this buffer has size 1
    * then return a buffer with that element repeated numElem times Otherwise
    * throw
    *
    * @param numElems
    */
  def broadcast(numElems: Int): BufferType

  def countNonMissing: Long = {
    var i = 0
    var s = 0
    val n = length
    while (i < n) {
      if (!isMissing(i)) { s += 1 }
      i += 1
    }
    s.toLong
  }

  def nonMissingMinMax: Option[(Elem,Elem)]
}

object BufferDouble {
  val MissingValue = Double.NaN
  def apply(s: Double*): BufferDouble = BufferDouble(s.toArray)
  def constant(value: Double, length: Int): BufferDouble =
    BufferDouble(Array.fill[Double](length)(value))
}

final case class BufferDouble(values: Array[Double])
    extends Buffer
    with BufferDoubleImpl { self =>

  type Elem = Double
  type BufferType = BufferDouble
  type SegmentType = SegmentDouble
  type ColumnType = Column.F64Column

  type ColumnTagType = ColumnTag.F64.type
  def tag: ColumnTagType = ColumnTag.F64

}

object BufferInt {
  val MissingValue = Int.MinValue
  def apply(e: Int*): BufferInt = apply(e.toArray)
  def apply(e: Array[Int]): BufferInt = {

    def isConstant = {
      var i = 1
      val x = e(0)
      var stop = false
      while (i < e.length && !stop) {
        if (x != e(i)) {
          stop = true
        }
        i += 1
      }
      !stop
    }

    if (e.isEmpty) empty
    else if (e.length == 1) single(e(0))
    else if (isConstant) constant(e(0), e.length)
    else
      BufferIntInArray(e)
  }
  def empty: BufferIntConstant = BufferIntConstant(Int.MinValue, 0)
  def single(value: Int): BufferIntConstant = BufferIntConstant(value, 1)
  def constant(value: Int, length: Int): BufferIntConstant =
    BufferIntConstant(value, length)

}

sealed trait BufferInt extends Buffer with Location with BufferIntImpl {
  type Elem = Int
  type BufferType = BufferInt
  type SegmentType = SegmentInt
  type ColumnType = Column.Int32Column

  type ColumnTagType = ColumnTag.I32.type
  def tag: ColumnTagType = ColumnTag.I32
  def raw(i: Int): Int
  def where(i: Int): BufferInt
  private[ra3] def values: Array[Int]

}

final case class BufferIntConstant(value: Int, length: Int)
    extends BufferInt
    with BufferIntConstantImpl {

  private[ra3] def values = Array.fill[Int](length)(value)
  def raw(i: Int): Int =
    if (i < 0 || i >= length) throw new IndexOutOfBoundsException else value
}

/* Buffer of Int, missing is Int.MinValue */
final case class BufferIntInArray(private val values0: Array[Int])
    extends BufferInt
    with BufferIntArrayImpl { self =>

  private[ra3] def values = values0
  def raw(i: Int): Int = values0(i)

}
object BufferLong {
  val MissingValue = Long.MinValue
  def apply(s: Long*): BufferLong = BufferLong(s.toArray)
  def constant(value: Long, length: Int): BufferLong =
    BufferLong(Array.fill[Long](length)(value))
}
/* Buffer of Lng, missing is Long.MinValue */
final case class BufferLong(private[ra3] val values: Array[Long])
    extends Buffer
    with BufferLongImpl { self =>
  type Elem = Long
  type BufferType = BufferLong
  type SegmentType = SegmentLong
  type ColumnType = Column.I64Column

  type ColumnTagType = ColumnTag.I64.type
  def tag: ColumnTagType = ColumnTag.I64

}
object BufferInstant {
  val MissingValue = Long.MinValue
  def apply(s: Long*): BufferInstant = BufferInstant(s.toArray)
  def constant(value: Long, length: Int): BufferInstant =
    BufferInstant(Array.fill[Long](length)(value))
}
/* Buffer of Lng, missing is Long.MinValue */
final case class BufferInstant(private[ra3] val values: Array[Long])
    extends Buffer
    with BufferInstantImpl { self =>
  type Elem = Long
  type BufferType = BufferInstant
  type SegmentType = SegmentInstant
  type ColumnType = Column.InstantColumn

  type ColumnTagType = ColumnTag.Instant.type
  def tag: ColumnTagType = ColumnTag.Instant

}
object BufferString {
  def apply(s: String*): BufferString = BufferString(s.toArray[CharSequence])
  val missing: CharSequence = s"${Char.MinValue}"
  val MissingValue = missing
  def constant(value: String, length: Int): BufferString =
    BufferString(Array.fill[CharSequence](length)(value))
}
/* Buffer of CharSequence, missing is null */
final case class BufferString(private[ra3] val values: Array[CharSequence])
    extends Buffer
    with BufferStringImpl { self =>
  type Elem = CharSequence
  type BufferType = BufferString
  type SegmentType = SegmentString
  type ColumnType = Column.StringColumn

  type ColumnTagType = ColumnTag.StringTag.type
  def tag: ColumnTagType = ColumnTag.StringTag

}

object Buffer {

  /** Map of unique items
    *
    * @param map
    *   An int buffer of the same length as the original buffer, each value is
    *   an integer in [0,n) where n is the number of unique items. All items in
    *   the original buffer share the same value in map
    * @param numGroups
    *   number of unique items
    * @param groupSizes
    *   number of repeats per unique items, the values of map indexes groupSizes
    */
  case class GroupMap(map: BufferInt, numGroups: Int, groupSizes: BufferInt)

  /** Returns an int buffer with the same number of elements. Each element is
    * [0,num), the group number in which that element belongs Also returns
    * number of groups Also returns a buffer with number of elements in each
    * group
    *
    * @param columns
    *   seq of columns, each column sequence of buffres
    */
  def computeGroups[B2 <: Buffer { type BufferType = B2 }](
      columns: Seq[Seq[B2]]
  ): GroupMap = {

    assert(columns.size > 0)
    if (columns.size == 1) {
      val buffers = columns.head.map(_.asBufferType)
      val concat = columns.head.head.tag.cat(buffers: _*)
      concat.groups
    } else {
      import org.saddle._
      import org.saddle.order._
      assert(columns.map(_.map(_.length.toLong).sum).distinct.size == 1,s"Got columns of multiple lengths: ${columns.map(_.map(_.length.toLong).sum).distinct}")
      val buffers = columns.map(buffers => buffers.head.tag.cat(buffers: _*))
      val factorizedEachBuffer = buffers.map(_.groups.map.toSeq).toVector
      // from here this is very inefficient because allocates too much
      // lamp has a more efficient implementation for this if I am willing to pull in lamp
      // (it delegates to pytorch uniques)
      val n = buffers.size

      val ar = Array.ofDim[Vector[Int]](factorizedEachBuffer.head.length)
      var i = 0
      while (i < ar.length) {
        val r = Array.ofDim[Int](n)
        var j = 0
        while (j < n) {
          r(j) = factorizedEachBuffer(j)(i)
          j += 1
        }
        ar(i) = r.toVector
        i += 1
      }
      val index = Index(ar)
      val uniques = index.uniques
      val counts = index.counts
      val map = Array.ofDim[Int](ar.length)
      i = 0
      while (i < ar.length) {
        map(i) = uniques.getFirst(ar(i))
        i += 1
      }
      GroupMap(
        map = BufferInt(map),
        numGroups = uniques.length,
        groupSizes = BufferInt(counts)
      )

    }

  }

  /** Returns an int buffer with the same number of elements. Each element is
    * [0,num), the partition number in which that element belongs
    */
  def computePartitions(buffers: Vector[Buffer], num: Int): BufferInt = {
    assert(buffers.nonEmpty)
    assert(buffers.map(_.length).distinct.size == 1)
    if (buffers.size == 1) {
      val r = Array.ofDim[Int](buffers.head.length)
      var i = 0
      val b = buffers.head
      while (i < r.length) {
        val hash = math.abs(b.hashOf(i))
        r(i) = (hash % num.toLong).toInt
        i += 1
      }
      BufferInt(r)
    } else {
      val r = Array.ofDim[Int](buffers.head.length)
      var i = 0
      val n = buffers.length
      while (i < r.length) {
        var j = 0
        var h = 0
        while (j < n) {
          h *= num
          h += (math.abs(buffers(j).hashOf(i)) % num.toLong).toInt
          j += 1
        }
        r(i) = h
        i += 1
      }
      BufferInt(r)
    }
  }

}
