package ra3

import cats.effect.IO
import java.nio.ByteBuffer
import java.nio.ByteOrder
import tasks.{TaskSystemComponents, SharedFile}

sealed trait Location
final case class Slice(start: Int, until: Int) extends Location

sealed trait Buffer[DType <: DataType] { //self: DType#BufferType =>
  // type SegType = DType#SegmentType

  def toSeq: Seq[DType#Elem]

  def cdf(numPoints: Int): (Buffer[DType], BufferInt)

  def groups: Buffer.GroupMap

  def length: Int

  def ++(b: DType#BufferType): DType#BufferType

  /* negative indices yield NA values */
  def take(locs: Location): Buffer[DType]

  /** takes element where mask is non missing and > 0, for string mask where non
    * missing and non empty
    */
  def filter(mask: Buffer[_]): Buffer[DType]

  /** uses the first element from cutoff */
  def filterInEquality[D2 <: DataType](
      cutoff: Buffer[D2],
      comparisonBuffer: Buffer[D2],
      less: Boolean
  ): Buffer[DType]

  def computeJoinIndexes(
      other: Buffer[DType],
      how: String
  ): (Option[BufferInt], Option[BufferInt])

  def mergeNonMissing(
      other: Buffer[DType]
  ): Buffer[DType]

  def isMissing(l: Int): Boolean
  def hashOf(l: Int): Int
  def toSegment(name: LogicalPath)(implicit
      tsc: TaskSystemComponents
  ): IO[Segment[DType]]

}

final class BufferDouble
    extends Buffer[F64] {

      override def toSeq: Seq[Double] = ???

      override def cdf(numPoints: Int): (Buffer[F64], BufferInt) = ???

      override def groups: Buffer.GroupMap = ???

      override def length: Int = ???

      override def ++(b: BufferDouble): BufferDouble = ???

      override def take(locs: Location): Buffer[F64] = ???

      override def filter(mask: Buffer[_]): Buffer[F64] = ???

      override def filterInEquality[D2 <: DataType](cutoff: Buffer[D2], comparisonBuffer: Buffer[D2], less: Boolean): Buffer[F64] = ???

      override def computeJoinIndexes(other: Buffer[F64], how: String): (Option[BufferInt], Option[BufferInt]) = ???

      override def mergeNonMissing(other: Buffer[F64]): Buffer[F64] = ???

      override def isMissing(l: Int): Boolean = ???

      override def hashOf(l: Int): Int = ???

      override def toSegment(name: LogicalPath)(implicit tsc: TaskSystemComponents): IO[SegmentDouble] = ???

    }

/* Buffer of Int, missing is Int.MinValue */
final case class BufferInt(private[ra3] val values: Array[Int])
    extends Buffer[Int32]
    with Location {



  def toSeq = values.toSeq

  def cdf(numPoints: Int): (Buffer[Int32], BufferInt) = {
    val percentiles =
      0 until (numPoints - 1) map (i => i * (1d / (numPoints - 1)))
    val idx = (percentiles.map(x => (x * (values.length - 1)).toInt) ++ List(
      values.length - 1
    )).distinct
    val sorted = org.saddle.array.sort[Int](values)
    val cdf = idx.map { idx =>
      (sorted(idx), idx)
    }
    val x = BufferInt(cdf.map(_._1).toArray)
    val y = BufferInt(cdf.map(_._2).toArray)
    (x, y)
  }

  def length = values.length

  import org.saddle.{Buffer => _, _}
  def filterInEquality[D2 <: DataType](
      cutoff: Buffer[D2],
      comparison: Buffer[D2],
      lessThan: Boolean
  ): Buffer[Int32] = {
    cutoff match {
      case BufferInt(values) =>
        val c = values(0)
        val idx =
          if (lessThan)
            comparison.asInstanceOf[BufferInt].values.toVec.find(_ <= c)
          else comparison.asInstanceOf[BufferInt].values.toVec.find(_ >= c)
        BufferInt(values.toVec.take(idx.toArray).toArray)

    }
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
      other: Buffer[Int32]
  ): BufferInt = {
    
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
      other: Buffer[Int32],
      how: String
  ): (Option[BufferInt], Option[BufferInt]) = {
    val idx1 = Index(values)
    val idx2 = other match {
      case BufferInt(values) => Index(values)
      case _                 => ???
    }
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
    values.toVec.concat(b.asInstanceOf[BufferInt].values.toVec).toArray
  )

  /** Returns an array of indices */
  def where(i: Int): BufferInt = {
    BufferInt(values.toVec.find(_ == i).toArray)
  }

  def filter(mask: Buffer[_]): BufferInt = mask match {
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
    */
  def computeGroups[D<:DataType](buffers: Seq[Buffer[D]]): GroupMap = {
    assert(buffers.size > 0)
    if (buffers.size == 1) {
      val buffer = buffers.head
      buffer.groups
    } else {
      import org.saddle._
      import org.saddle.order._
      assert(buffers.map(_.length).distinct == 1)
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
  def computePartitions(buffers: Seq[Buffer[_]], num: Int): BufferInt = {
    assert(buffers.nonEmpty)
    assert(buffers.map(_.length).distinct.size == 1)
    if (buffers.size == 1) {
      val r = Array.ofDim[Int](buffers.head.length)
      var i = 0
      val b = buffers.head
      while (i < r.length) {
        val hash = b.hashOf(i)
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
        val hash = hashing.hash(hashes)
        r(i) = hash % num
        i += 1
      }
      BufferInt(r)
    }
  }

}
