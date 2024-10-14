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

private[ra3] sealed trait Location
private[ra3] final case class Slice(start: Int, until: Int) extends Location

private[ra3] sealed trait TaggedBuffers { self =>
  type Elem
  type BufferType <: Buffer
  val tag: ColumnTag {
    type BufferType = self.BufferType
  }
  def buffers: Seq[tag.BufferType]
}
private[ra3] sealed trait TaggedBuffer { self =>
  type Elem
  type BufferType <: Buffer
  val tag: ColumnTag {
    type BufferType = self.BufferType
  }
  def buffer: tag.BufferType
}

private[ra3] case class TaggedBuffersI32(buffers: Seq[BufferInt])
    extends TaggedBuffers {
  val tag = ColumnTag.I32
  type BufferType = BufferInt
  type Elem = Int
}
private[ra3] case class TaggedBuffersI64(buffers: Seq[BufferLong])
    extends TaggedBuffers {
  val tag = ColumnTag.I64
  type BufferType = BufferLong
  type Elem = Long
}
private[ra3] case class TaggedBuffersF64(buffers: Seq[BufferDouble])
    extends TaggedBuffers {
  val tag = ColumnTag.F64
  type BufferType = BufferDouble
  type Elem = Double
}
private[ra3] case class TaggedBuffersInstant(buffers: Seq[BufferInstant])
    extends TaggedBuffers {
  val tag = ColumnTag.Instant
  type BufferType = BufferInstant
  type Elem = Long
}
private[ra3] case class TaggedBuffersString(buffers: Seq[BufferString])
    extends TaggedBuffers {
  val tag = ColumnTag.StringTag
  type BufferType = BufferString
  type Elem = CharSequence
}

private[ra3] sealed trait Buffer {
  type Elem

  /** Returns the items in the buffer as a Seq
    *
    * Missing values are encoded onto the same value as used internally
    */
  def toSeq: Seq[Elem]

  def toArray: Array[Elem]

  def element(i:Int) : Elem

  def elementAsCharSequence(i: Int): CharSequence

  /** Returns a map of unique items */
  def groups: Buffer.GroupMap

  def length: Int

  // /** negative indices yield NA values */
  // def take(locs: Location): BufferType

  /** returns indexes where value is positive (or positive length) */
  def positiveLocations: BufferInt

  /** Returns true if item at position l is missing. Throws if out of bounds */
  def isMissing(l: Int): Boolean

  /** Returns hash of item at location l. Missing values get the same hash.
    * Throws if out of bounds
    */
  def hashOf(l: Int): Long

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

  def nonMissingMinMax: Option[(Elem, Elem)]
}

private[ra3] object BufferDouble {
  val MissingValue = Double.NaN
  def apply(s: Double*): BufferDouble = BufferDouble(s.toArray)
  def constant(value: Double, length: Int): BufferDouble =
    BufferDouble(Array.fill[Double](length)(value))
}

private[ra3] final case class BufferDouble(values: Array[Double])
    extends Buffer
    with BufferDoubleImpl
    with TaggedBuffer { self =>
    
  def toArray = values

  type Elem = Double
  type BufferType = BufferDouble
  val tag: ColumnTag.F64.type = ColumnTag.F64
  def buffer = this
}

private[ra3] object BufferInt {
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

private[ra3] sealed trait BufferInt
    extends Buffer
    with Location
    with BufferIntImpl
    with TaggedBuffer {
  type Elem = Int
  type BufferType = BufferInt

  val tag: ColumnTag.I32.type = ColumnTag.I32
  def raw(i: Int): Int
  def where(i: Int): BufferInt
  private[ra3] def values: Array[Int]

}

private[ra3] final case class BufferIntConstant(value: Int, length: Int)
    extends BufferInt
    with BufferIntConstantImpl {
  def buffer = this

  def toArray = Array.fill[Int](length)(value)
  private[ra3] def values = Array.fill[Int](length)(value)
  def raw(i: Int): Int =
    if (i < 0 || i >= length) throw new IndexOutOfBoundsException else value
}

/* Buffer of Int, missing is Int.MinValue */
private[ra3] final case class BufferIntInArray(private val values0: Array[Int])
    extends BufferInt
    with BufferIntArrayImpl { self =>
  def buffer = this
  def toArray = values0
  private[ra3] def values = values0
  def raw(i: Int): Int = values0(i)

}
private[ra3] object BufferLong {
  val MissingValue = Long.MinValue
  def apply(s: Long*): BufferLong = BufferLong(s.toArray)
  def constant(value: Long, length: Int): BufferLong =
    BufferLong(Array.fill[Long](length)(value))
}
/* Buffer of Long, missing is Long.MinValue */
private[ra3] final case class BufferLong(private[ra3] val values: Array[Long])
    extends Buffer
    with BufferLongImpl
    with TaggedBuffer { self =>
  def buffer = this
  def toArray = values
  type Elem = Long
  type BufferType = BufferLong
  val tag: ColumnTag.I64.type = ColumnTag.I64

}
private[ra3] object BufferInstant {
  val MissingValue = Long.MinValue
  
  def apply(s: Long*): BufferInstant = BufferInstant(s.toArray)
  def constant(value: Long, length: Int): BufferInstant =
    BufferInstant(Array.fill[Long](length)(value))
}
/* Buffer of Lng, missing is Long.MinValue */
private[ra3] final case class BufferInstant(
    private[ra3] val values: Array[Long]
) extends Buffer
    with BufferInstantImpl
    with TaggedBuffer { self =>
  def buffer = this
  def toArray = values
  type Elem = Long
  type BufferType = BufferInstant

  val tag: ColumnTag.Instant.type = ColumnTag.Instant

}
private[ra3] object BufferString {
  def apply(s: String*): BufferString = BufferString(s.toArray[CharSequence])
  val missing: CharSequence = s"${Char.MinValue}"
  val MissingValue = missing
  def constant(value: String, length: Int): BufferString =
    BufferString(Array.fill[CharSequence](length)(value))
}
/* Buffer of CharSequence, missing is null */
private[ra3] final case class BufferString(
    private[ra3] val values: Array[CharSequence]
) extends Buffer
    with BufferStringImpl
    with TaggedBuffer { self =>
  def buffer = this
  def toArray = values
  type Elem = CharSequence
  type BufferType = BufferString
  val tag: ColumnTag.StringTag.type = ColumnTag.StringTag
}

private[ra3] object Buffer {

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
    * number of groups. Also returns a buffer with number of elements in each
    * group
    *
    * @param columns
    *   seq of columns, each column sequence of buffres
    */
  def computeGroups(
      columns: Seq[TaggedBuffers]
  ): GroupMap = {

    assert(columns.size > 0)
    if (columns.size == 1) {
      val buffers = columns.head // .map(_.asBufferType)
      val tag = buffers.tag
      val concat = tag.cat(buffers.buffers*)
      concat.groups
    } else {
      // import org.saddle.*
      // import org.saddle.order.*
      assert(
        columns.map(v => v.buffers.map(_.length.toLong).sum).distinct.size == 1,
        s"Got columns of multiple lengths: ${columns.map(v => v.buffers.map(_.length.toLong).sum).distinct}"
      )
      val buffers = columns.map(buffers => buffers.tag.cat(buffers.buffers*))
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
      val index = ra3.join.locator.LocatorAny.fromKeys(ar)
      val uniques = index.uniqueKeys
    val uniquesLoc = ra3.join.locator.LocatorAny.fromKeys(uniques)
      val counts = index.counts
      val map = Array.ofDim[Int](ar.length)
      i = 0
      while (i < ar.length) {
        map(i) = uniquesLoc.get(ar(i))
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
