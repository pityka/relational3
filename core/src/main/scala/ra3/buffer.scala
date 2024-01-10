package ra3

import cats.effect.IO
import java.nio.ByteBuffer
import java.nio.ByteOrder
import tasks.{TaskSystemComponents, SharedFile}

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

  def as(b: Buffer) = this.asInstanceOf[b.BufferType]
  def as(b: ColumnTag) = this.asInstanceOf[b.BufferType]

  /* given the bounds on BufferType this should never fail  */
  def asBufferType = this.asInstanceOf[BufferType]

  def toSeq: Seq[Elem]
  def findInequalityVsHead(other: BufferType, lessThan: Boolean): BufferInt

  def cdf(numPoints: Int): (BufferType, BufferDouble)

  def groups: Buffer.GroupMap

  def length: Int

  def ++(b: BufferType): BufferType

  /* negative indices yield NA values */
  def take(locs: Location): BufferType

  /** takes element where mask is non missing and > 0, for string mask where non
    * missing and non empty
    */
  def filter(mask: Buffer): BufferType

  /** uses the first element from cutoff */
  def filterInEquality[
      B0 <: Buffer,
      B <: Buffer { type BufferType = B0 }
  ](
      comparison: B,
      cutoff: B,
      less: Boolean
  ): BufferType

  def computeJoinIndexes(
      other: BufferType,
      how: String
  ): (Option[BufferInt], Option[BufferInt])

  def mergeNonMissing(
      other: BufferType
  ): BufferType

  def isMissing(l: Int): Boolean
  def hashOf(l: Int): Int
  def toSegment(name: LogicalPath)(implicit
      tsc: TaskSystemComponents
  ): IO[SegmentType]

  def sumGroups(partitionMap: BufferInt, numGroups: Int): BufferType

}

final case class BufferDouble(values: Array[Double]) extends Buffer { self =>

  def sumGroups(partitionMap: BufferInt, numGroups: Int): BufferType = ???

  override def findInequalityVsHead(
      other: BufferType,
      lessThan: Boolean
  ): BufferInt = ???

  type Elem = Double
  type BufferType = BufferDouble
  type SegmentType = SegmentDouble
  type ColumnType = Column.F64Column

  def filterInEquality[
      B0 <: Buffer,
      B <: Buffer { type BufferType = B0 }
  ](
      comparison: B,
      cutoff: B,
      less: Boolean
  ): BufferType = ???

  override def toSeq: Seq[Double] = values.toSeq

  override def cdf(numPoints: Int): (BufferDouble, BufferDouble) = ???

  override def groups: Buffer.GroupMap = ???

  override def length: Int = values.length

  override def ++(b: BufferDouble): BufferDouble = BufferDouble(values ++ b.values)

  override def take(locs: Location): BufferDouble = ???

  override def filter(mask: Buffer): BufferDouble = ???

  override def computeJoinIndexes(
      other: BufferType,
      how: String
  ): (Option[BufferInt], Option[BufferInt]) = ???

  def mergeNonMissing(
      other: BufferType
  ): BufferType = ???

  override def isMissing(l: Int): Boolean = values(l).isNaN

  override def hashOf(l: Int): Int = ???

  override def toSegment(name: LogicalPath)(implicit
      tsc: TaskSystemComponents
  ): IO[SegmentDouble] =   IO {
      val bb =
        ByteBuffer.allocate(8 * values.length).order(ByteOrder.LITTLE_ENDIAN)
      bb.asDoubleBuffer().put(values)
      fs2.Stream.chunk(fs2.Chunk.byteBuffer(bb))
    }.flatMap { stream =>
      SharedFile
        .apply(stream, name.toString)
        .map(sf => SegmentDouble(sf, values.length))
    }

}

/* Buffer of Int, missing is Int.MinValue */
final case class BufferInt(private[ra3] val values: Array[Int])
    extends Buffer
    with Location { self =>

  override def toString =
    s"BufferInt(n=${values.length}: ${values.take(5).mkString(", ")} ..})"

  type Elem = Int
  type BufferType = BufferInt
  type SegmentType = SegmentInt
  type ColumnType = Column.Int32Column

  /* Returns a buffer of numGroups. It may overflow. */
  def sumGroups(partitionMap: BufferInt, numGroups: Int): BufferType = {
    val ar = Array.ofDim[Int](numGroups)
    var i = 0
    val n = partitionMap.values.length
    while (i < n) {
      ar(partitionMap.values(i)) += values(i)
      i += 1
    }
    BufferInt(ar)

  }

  /** Find locations at which _ <= other[0] or _ >= other[0] holds returns
    * indexes
    */
  override def findInequalityVsHead(
      other: BufferType,
      lessThan: Boolean
  ): BufferInt = {
    import org.saddle._
    val c = other.values(0)
    val idx =
      if (lessThan)
        values.toVec.find(_ <= c)
      else values.toVec.find(_ >= c)

    BufferInt(idx.toArray)
  }

  def toSeq = values.toSeq

  def cdf(numPoints: Int): (BufferInt, BufferDouble) = {
    val percentiles =
      ((0 until (numPoints - 1)).map(i => i * (1d / (numPoints - 1))) ++ List(1d)).distinct
    val sorted = org.saddle.array.sort[Int](values)
    val cdf = percentiles.map { p =>
      val idx = (p * (values.length - 1)).toInt
      (sorted(idx), p)
    }//.groupBy(_._1).toSeq.map(v => (v._1,v._2.map(_._2).min)).sortBy(_._1)
    
    val x = BufferInt(cdf.map(_._1).toArray)
    val y = BufferDouble(cdf.map(_._2).toArray)
    (x, y)
  }

  def length = values.length

  import org.saddle.{Buffer => _, _}

  override def filterInEquality[
      B0 <: Buffer,
      B <: Buffer { type BufferType = B0 }
  ](comparison: B, cutoff: B, less: Boolean): BufferInt = {
    val idx = comparison.findInequalityVsHead(cutoff.asBufferType, less)

    BufferInt(values.toVec.take(idx.values.toArray).toArray)
  }

  def groups = {
    val idx = Index(values)
    val uniques = idx.uniques
    val counts = idx.counts
    val map = Array.ofDim[Int](values.length)
    var i = 0
    while (i < values.length) {
      map(i) = uniques.getFirst(values(i))
      i += 1
    }
    Buffer.GroupMap(
      map = BufferInt(map),
      numGroups = uniques.length,
      groupSizes = BufferInt(counts)
    )
  }

  def mergeNonMissing(
      other: BufferType
  ): BufferType = {

    other match {
      case BufferInt(otherValues) =>
        assert(values.length == otherValues.length)
        var i = 0
        val r = Array.ofDim[Int](values.length)
        while (i < values.length) {
          r(i) = values(i)
          if (isMissing(i)) {
            r(i) = otherValues(i)
          }
          i += 1
        }
        BufferInt(r)
      case _ => ???

    }
  }

  def computeJoinIndexes(
      other: BufferType,
      how: String
  ): (Option[BufferInt], Option[BufferInt]) = {
    val idx1 = Index(values)
    val idx2 = Index(other.asBufferType.values)
    val reindexer = idx1.join(
      idx2,
      how = how match {
        case "inner" => org.saddle.index.InnerJoin
        case "left"  => org.saddle.index.LeftJoin
        case "right" => org.saddle.index.RightJoin
        case "outer" => org.saddle.index.OuterJoin
      }
    )
    (reindexer.lTake.map(BufferInt(_)), reindexer.rTake.map(BufferInt(_)))
  }

  def ++(b: BufferInt) = BufferInt(
    values.toVec.concat(b.values.toVec).toArray
  )

  /** Returns an array of indices */
  def where(i: Int): BufferInt = {
    BufferInt(values.toVec.find(_ == i).toArray)
  }

  def filter(mask: Buffer): BufferInt = mask match {
    case BufferInt(mask) =>
      BufferInt(values.toVec.take(mask.toVec.find(_ > 0).toArray).toArray)
  }

  override def take(locs: Location): BufferInt = locs match {
    case Slice(start, until) =>
      val r = Array.ofDim[Int](until - start)
      System.arraycopy(values, start, r, 0, until - start)
      BufferInt(r)
    case BufferInt(idx) =>
      BufferInt(values.toVec.take(idx).toArray)
  }

  override def isMissing(l: Int): Boolean = {
    values(l) == Int.MinValue
  }
  override def hashOf(l: Int): Int = {
    values(l)
  }

  override def toSegment(
      name: LogicalPath
  )(implicit tsc: TaskSystemComponents): IO[SegmentInt] = {
    IO {
      val bb =
        ByteBuffer.allocate(4 * values.length).order(ByteOrder.LITTLE_ENDIAN)
      bb.asIntBuffer().put(values)
      fs2.Stream.chunk(fs2.Chunk.byteBuffer(bb))
    }.flatMap { stream =>
      SharedFile
        .apply(stream, name.toString)
        .map(sf => SegmentInt(sf, values.length))
    }

  }

}

object Buffer {
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
      val concat = buffers.reduce(_ ++ _)
      concat.groups
    } else {
      import org.saddle._
      import org.saddle.order._
      assert(columns.map(_.map(_.length.toLong).sum).distinct == 1)
      val buffers = columns.map(_.reduce(_ ++ _))
      val factorizedEachBuffer = buffers.map(_.groups.map.values).toVector
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
  def computePartitions(buffers: Seq[Buffer], num: Int): BufferInt = {
    assert(buffers.nonEmpty)
    assert(buffers.map(_.length).distinct.size == 1)
    if (buffers.size == 1) {
      val r = Array.ofDim[Int](buffers.head.length)
      var i = 0
      val b = buffers.head
      while (i < r.length) {
        val hash = math.abs(b.hashOf(i))
        r(i) = hash % num
        i += 1
      }
      BufferInt(r)
    } else {
      val r = Array.ofDim[Int](buffers.head.length)
      var i = 0
      val hashing = scala.util.hashing.MurmurHash3.arrayHashing[Int]
      while (i < r.length) {
        val hashes = buffers.map(_.hashOf(i)).toArray
        val hash = math.abs(hashing.hash(hashes))
        r(i) = hash % num
        i += 1
      }
      BufferInt(r)
    }
  }

}
