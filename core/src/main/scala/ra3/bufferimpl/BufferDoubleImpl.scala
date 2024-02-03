package ra3.bufferimpl
import ra3._
import cats.effect.IO
import java.nio.ByteBuffer
import java.nio.ByteOrder
import tasks.{TaskSystemComponents, SharedFile}
private[ra3] trait BufferDoubleImpl { self: BufferDouble =>
  override def toSeq: Seq[Double] = values.toSeq

  def broadcast(n: Int) = self.length match {
    case x if x == n => this
    case 1 =>
      BufferDouble(Array.fill[Double](n)(values(0)))
    case _ =>
      throw new RuntimeException("broadcast called on buffer with wrong size")
  }

  def sumGroups(partitionMap: BufferInt, numGroups: Int): BufferType = {
    val ar = Array.fill[Double](numGroups)(Double.NaN)
    var i = 0
    val n = partitionMap.length
    while (i < n) {
      if (!isMissing(i)) {
        if (ar(partitionMap.raw(i)).isNaN()) {
          ar(partitionMap.raw(i)) = values(i)
        } else ar(partitionMap.raw(i)) += values(i)
      }
      i += 1
    }
    BufferDouble(ar)

  }

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

  override def cdf(numPoints: Int): (BufferDouble, BufferDouble) = {
    val percentiles =
      ((0 until (numPoints - 1)).map(i => i * (1d / (numPoints - 1))) ++ List(
        1d
      )).distinct
    val sorted = org.saddle.array.sort[Double](values)
    val cdf = percentiles.map { p =>
      val idx = (p * (values.length - 1)).toInt
      (sorted(idx), p)
    }

    val x = BufferDouble(cdf.map(_._1).toArray)
    val y = BufferDouble(cdf.map(_._2).toArray)
    (x, y)
  }

  override def groups: Buffer.GroupMap = {
    import org.saddle._
    val idx = Index(values)
    val uniques = idx.uniques
    val counts = idx.counts
    val map = Array.ofDim[Int](values.length)
    var i = 0
    while (i < values.length) {
      map(i) = uniques.getFirst(values(i))
      i += 1
    }
    ra3.Buffer.GroupMap(
      map = BufferInt(map),
      numGroups = uniques.length,
      groupSizes = BufferInt(counts)
    )
  }

  override def length: Int = values.length

  override def take(locs: Location): BufferDouble = locs match {
    case Slice(start, until) =>
      val r = Array.ofDim[Double](until - start)
      System.arraycopy(values, start, r, 0, until - start)
      BufferDouble(r)
    case idx: BufferInt =>
      import org.saddle._
      BufferDouble(values.toVec.take(idx.values).toArray)
  }

  def positiveLocations: BufferInt = {
    import org.saddle._
    BufferInt(
      values.toVec.find(_ > 0).toArray
    )
  }

  override def computeJoinIndexes(
      other: BufferType,
      how: String
  ): (Option[BufferInt], Option[BufferInt]) = {
    import org.saddle._
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

  def mergeNonMissing(
      other: BufferType
  ): BufferType = {
    val otherValues = other.values
    assert(values.length == otherValues.length)
    var i = 0
    val r = Array.ofDim[Double](values.length)
    while (i < values.length) {
      r(i) = values(i)
      if (isMissing(i)) {
        r(i) = otherValues(i)
      }
      i += 1
    }
    BufferDouble(r)
  }

  override def isMissing(l: Int): Boolean = values(l).isNaN

  override def hashOf(l: Int): Long = {
    java.lang.Double.doubleToLongBits(values(l))
  }

  def minMax = if (values.length == 0) None
  else {
    import org.saddle._
    Some(
      (
        if (values.toVec.hasNA) Double.NaN else values.toVec.min2,
        values.toVec.max.get
      )
    )
  }

  def firstInGroup(partitionMap: BufferInt, numGroups: Int): BufferType = {
    assert(partitionMap.length == length)
    val ar = Array.fill(numGroups)(Double.NaN)
    var i = 0
    val n = partitionMap.length
    while (i < n) {
      if (!isMissing(i)) { ar(partitionMap.raw(i)) = values(i) }
      i += 1
    }
    tag.makeBuffer(ar)

  }

  override def toSegment(name: LogicalPath)(implicit
      tsc: TaskSystemComponents
  ): IO[SegmentDouble] =
    if (values.length == 0) IO.pure(SegmentDouble(None, 0, None))
    else
      IO {

        val bb =
          ByteBuffer.allocate(8 * values.length).order(ByteOrder.LITTLE_ENDIAN)
        bb.asDoubleBuffer().put(values)
        fs2.Stream.chunk(fs2.Chunk.byteBuffer(bb))
      }.flatMap { stream =>
        val minmax = self.minMax

        SharedFile
          .apply(stream, name.toString)
          .map(sf => SegmentDouble(Some(sf), values.length, minmax))
      }

  def elementwise_*=(other: BufferType): Unit = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    while (i < n) {
      self.values(i) = self.values(i) * other.values(i)
      i += 1
    }
  }
  def elementwise_*=(other: BufferLong): Unit = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    while (i < n) {
      self.values(i) = self.values(i) * other.values(i).toDouble
      i += 1
    }
  }
  def elementwise_+=(other: BufferType): Unit = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    while (i < n) {
      self.values(i) = self.values(i) + other.values(i)
      i += 1
    }
  }
  def elementwise_+=(other: BufferLong): Unit = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    while (i < n) {
      self.values(i) = self.values(i) + other.values(i).toDouble
      i += 1
    }
  }
  def elementwise_eq(other: BufferType): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) = if (self.values(i) == other.values(i).toDouble) 1 else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_gt(other: BufferType): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) = if (self.values(i) > other.values(i).toDouble) 1 else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_gteq(other: BufferType): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) = if (self.values(i) >= other.values(i).toDouble) 1 else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_lt(other: BufferType): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) = if (self.values(i) < other.values(i).toDouble) 1 else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_lteq(other: BufferType): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) = if (self.values(i) <= other.values(i).toDouble) 1 else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_neq(other: BufferType): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) = if (self.values(i) != other.values(i).toDouble) 1 else 0
      i += 1
    }
    BufferInt(r)
  }

  def elementwise_eq(other: BufferLong): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) = if (self.values(i) == other.values(i).toDouble) 1 else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_gt(other: BufferLong): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) = if (self.values(i) > other.values(i).toDouble) 1 else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_gteq(other: BufferLong): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) = if (self.values(i) >= other.values(i).toDouble) 1 else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_lt(other: BufferLong): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) = if (self.values(i) < other.values(i).toDouble) 1 else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_lteq(other: BufferLong): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) = if (self.values(i) <= other.values(i).toDouble) 1 else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_neq(other: BufferLong): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) = if (self.values(i) != other.values(i).toDouble) 1 else 0
      i += 1
    }
    BufferInt(r)
  }

  def elementwise_roundToLong: BufferLong = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Long](n)
    while (i < n) {
      r(i) = self.values(i).toLong
      i += 1
    }
    BufferLong(r)
  }
}
