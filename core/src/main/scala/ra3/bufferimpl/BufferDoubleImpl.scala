package ra3.bufferimpl
import ra3.*
import cats.effect.IO
import java.nio.ByteBuffer
import java.nio.ByteOrder
import tasks.{TaskSystemComponents, SharedFile}
import ra3.join.*
import ra3.join.locator.LocatorDouble
private[ra3] trait BufferDoubleImpl { self: BufferDouble =>

  def nonMissingMinMax = makeStatistic().nonMissingMinMax
  def makeStatistic() = {
    var i = 0
    val n = length
    var hasMissing = false
    var countNonMissing = 0
    var min = Double.MaxValue
    var max = Double.MinValue
    val set = scala.collection.mutable.Set.empty[Double]
    while (i < n) {
      if (isMissing(i)) {
        hasMissing = true
      } else {
        countNonMissing += 1
        val v = values(i)
        if (v < min) {
          min = v
        }
        if (v > max) {
          max = v
        }
        if (set.size < 256 && !set.contains(v)) {
          set.+=(v)
        }
      }
      i += 1
    }
    StatisticDouble(
      hasMissing = hasMissing,
      nonMissingMinMax = if (countNonMissing > 0) Some((min, max)) else None,
      lowCardinalityNonMissingSet =
        if (set.size <= 255) Some(set.toSet) else None
    )
  }

  override def toSeq: Seq[Double] = values.toSeq

  def element(i: Int): Double = values(i)

  def elementAsCharSequence(i: Int): CharSequence = values(i).toString

  def partition(numPartitions: Int, map: BufferInt): Vector[BufferType] = {
    assert(length == map.length)
    val growableBuffers =
      Vector.fill(numPartitions)(MutableBuffer.emptyD)
    var i = 0
    val n = length
    val mapv = map.values
    while (i < n) {
      growableBuffers(mapv(i)).+=(values(i))
      i += 1
    }
    growableBuffers.map(v => BufferDouble(v.toArray))
  }

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
  def minInGroups(partitionMap: BufferInt, numGroups: Int): BufferType = {
    val ar = Array.fill[Double](numGroups)(Double.NaN)
    var i = 0
    val n = partitionMap.length
    while (i < n) {
      if (!isMissing(i)) {
        val g = partitionMap.raw(i)
        if (ar(g).isNaN()) {
          ar(g) = values(i)
        } else if (ar(g) > values(i)) {
          ar(g) = values(i)
        }
      }
      i += 1
    }
    BufferDouble(ar)

  }
  def maxInGroups(partitionMap: BufferInt, numGroups: Int): BufferType = {
    val ar = Array.fill[Double](numGroups)(Double.NaN)
    var i = 0
    val n = partitionMap.length
    while (i < n) {
      if (!isMissing(i)) {
        val g = partitionMap.raw(i)
        if (ar(g).isNaN()) {
          ar(g) = values(i)
        } else if (ar(g) < values(i)) {
          ar(g) = values(i)
        }
      }
      i += 1
    }
    BufferDouble(ar)

  }
  def hasMissingInGroup(partitionMap: BufferInt, numGroups: Int): BufferInt = {
    val ar = Array.fill[Int](numGroups)(0)
    var i = 0
    val n = partitionMap.length
    while (i < n) {
      val g = partitionMap.raw(i)
      if (ar(g) == 0 && isMissing(i)) {
        ar(g) = 1
      }
      i += 1
    }
    BufferInt(ar)

  }
  def countGroups(partitionMap: BufferInt, numGroups: Int): BufferType = {
    val ar = Array.fill[Double](numGroups)(Double.NaN)
    var i = 0
    val n = partitionMap.length
    while (i < n) {
      if (!isMissing(i)) {
        if (ar(partitionMap.raw(i)).isNaN()) {
          ar(partitionMap.raw(i)) = 1d
        } else ar(partitionMap.raw(i)) += 1d
      }
      i += 1
    }
    BufferDouble(ar)

  }
  def countDistinctGroups(
      partitionMap: BufferInt,
      numGroups: Int
  ): BufferInt = {
    // need to convert to long bits due to NaN failing all comparisons
    val ar = Array.fill[scala.collection.mutable.Set[Long]](numGroups)(
      scala.collection.mutable.Set.empty[Long]
    )
    var i = 0
    val n = partitionMap.length
    while (i < n) {

      ar(partitionMap.raw(i)).add(java.lang.Double.doubleToLongBits(values(i)))

      i += 1
    }
    BufferInt(ar.map(_.size))

  }
  def meanGroups(partitionMap: BufferInt, numGroups: Int): BufferType = {
    val ar = Array.fill[Double](numGroups)(Double.NaN)
    val arCount = Array.ofDim[Int](numGroups)
    var i = 0
    val n = partitionMap.length
    while (i < n) {
      if (!isMissing(i)) {
        if (ar(partitionMap.raw(i)).isNaN()) {
          ar(partitionMap.raw(i)) = values(i)
          arCount(partitionMap.raw(i)) = 1
        } else {
          ar(partitionMap.raw(i)) += values(i)
          arCount(partitionMap.raw(i)) += 1
        }
      }
      i += 1
    }
    var j = 0
    val m = numGroups
    while (j < m) {
      ar(j) /= arCount(j)
      j += 1
    }
    BufferDouble(ar)

  }

  def findInequalityVsHead(
      other: BufferType,
      lessThan: Boolean
  ): BufferInt = {
    val c = other.values(0)
    if (c.isNaN) BufferInt.empty
    else {
      val idx =
        if (lessThan)
          ArrayUtil.findD(values, _ <= c)
        else ArrayUtil.findD(values, _ >= c)

      BufferInt(idx.toArray)
    }
  }

  def cdf(numPoints: Int): (BufferDouble, BufferDouble) = {
    val percentiles =
      ((0 until (numPoints - 1)).map(i => i * (1d / (numPoints - 1))) ++ List(
        1d
      )).distinct
    val sorted: Array[Double] = {
      val cpy = ArrayUtil.dropNAD(values)
      java.util.Arrays.sort(cpy)
      cpy
    }
    val cdf = percentiles.map { p =>
      val idx = (p * (sorted.length - 1)).toInt
      (sorted(idx), p)
    }

    val x = BufferDouble(cdf.map(_._1).toArray)
    val y = BufferDouble(cdf.map(_._2).toArray)
    (x, y)
  }

  override def groups: Buffer.GroupMap = {
    val (counts, uniqueIdx) = ra3.hashtable.DoubleTable
      .buildWithUniques(values, Array.ofDim[Int](values.length))

    val map = Array.ofDim[Int](values.length)
    var i = 0
    while (i < values.length) {
      counts.mutate(values(i), _ + 1)
      i += 1
    }
    i = 0
    val groups = uniqueIdx.map(values)
    val groupmap = ra3.hashtable.GenericTable.buildFirst(groups, null)
    while (i < values.length) {
      map(i) = groupmap.lookupIdx(values(i))
      i += 1
    }

    val c = groups.map(cs => counts.payload(counts.lookupIdx(cs)))
    Buffer.GroupMap(
      map = BufferInt(map),
      numGroups = uniqueIdx.length,
      groupSizes = BufferInt(c)
    )
  }

  override def length: Int = values.length

  def take(locs: Location): BufferDouble = locs match {
    case Slice(start, until) =>
      val r = Array.ofDim[Double](until - start)
      System.arraycopy(values, start, r, 0, until - start)
      BufferDouble(r)
    case idx: BufferInt =>
      BufferDouble(ArrayUtil.takeD(values, idx.values))
  }

  def positiveLocations: BufferInt = {
    BufferInt(
      ArrayUtil.findD(values, _ > 0)
    )
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
    scala.util.hashing.byteswap64(java.lang.Double.doubleToLongBits(values(l)))
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

  def toSegment(name: LogicalPath)(implicit
      tsc: TaskSystemComponents
  ): IO[SegmentDouble] =
    if (values.length == 0)
      IO.pure(SegmentDouble(None, 0, StatisticDouble.empty))
    else
      IO.cede >> (IO {

        val bb =
          ByteBuffer.allocate(8 * values.length).order(ByteOrder.LITTLE_ENDIAN)
        bb.asDoubleBuffer().put(values)
        fs2.Stream.chunk(fs2.Chunk.byteBuffer(Utils.compress(bb,skipCompress=true)))
      }.flatMap { stream =>
        SharedFile
          .apply(stream, name.toString)
          .map(sf =>
            SegmentDouble(Some(sf), values.length, self.makeStatistic())
          )
      }).logElapsed.guarantee(IO.cede)

  def elementwise_eq(other: BufferType): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || other.isMissing(i)) BufferInt.MissingValue
        else if (self.values(i) == other.values(i)) 1
        else 0
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
      r(i) =
        if (isMissing(i) || other.isMissing(i)) BufferInt.MissingValue
        else if (self.values(i) > other.values(i)) 1
        else 0
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
      r(i) =
        if (isMissing(i) || other.isMissing(i)) BufferInt.MissingValue
        else if (self.values(i) >= other.values(i)) 1
        else 0
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
      r(i) =
        if (isMissing(i) || other.isMissing(i)) BufferInt.MissingValue
        else if (self.values(i) < other.values(i)) 1
        else 0
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
      r(i) =
        if (isMissing(i) || other.isMissing(i)) BufferInt.MissingValue
        else if (self.values(i) <= other.values(i)) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_lteq(other: Double): BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || other.isNaN) BufferInt.MissingValue
        else if (self.values(i) <= other) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_gteq(other: Double): BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || other.isNaN) BufferInt.MissingValue
        else if (self.values(i) >= other) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_gt(other: Double): BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || other.isNaN) BufferInt.MissingValue
        else if (self.values(i) > other) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_lt(other: Double): BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || other.isNaN) BufferInt.MissingValue
        else if (self.values(i) < other) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_eq(other: Double): BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || other.isNaN) BufferInt.MissingValue
        else if (self.values(i) == other) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_neq(other: Double): BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || other.isNaN) BufferInt.MissingValue
        else if (self.values(i) != other) 1
        else 0
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
      r(i) =
        if (isMissing(i) || other.isMissing(i)) BufferInt.MissingValue
        else if (self.values(i) != other.values(i)) 1
        else 0
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
      r(i) =
        if (isMissing(i) || other.isMissing(i)) BufferInt.MissingValue
        else if (self.values(i) == other.values(i)) 1
        else 0
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
      r(i) =
        if (isMissing(i) || other.isMissing(i)) BufferInt.MissingValue
        else if (self.values(i) > other.values(i)) 1
        else 0
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
      r(i) =
        if (isMissing(i) || other.isMissing(i)) BufferInt.MissingValue
        else if (self.values(i) >= other.values(i)) 1
        else 0
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
      r(i) =
        if (isMissing(i) || other.isMissing(i)) BufferInt.MissingValue
        else if (self.values(i) < other.values(i)) 1
        else 0
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
      r(i) =
        if (isMissing(i) || other.isMissing(i)) BufferInt.MissingValue
        else if (self.values(i) <= other.values(i)) 1
        else 0
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
      r(i) =
        if (isMissing(i) || other.isMissing(i)) BufferInt.MissingValue
        else if (self.values(i) != other.values(i)) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }

  def elementwise_roundToLong: BufferLong = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Long](n)
    while (i < n) {
      r(i) =
        if (isMissing(i)) BufferLong.MissingValue else self.values(i).toLong
      i += 1
    }
    BufferLong(r)
  }

  def elementwise_div(other: BufferType): BufferType = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Double](n)

    while (i < n) {
      r(i) = self.values(i) / other.values(i)
      i += 1
    }
    BufferDouble(r)
  }
  def elementwise_mul(other: BufferType): BufferType = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Double](n)

    while (i < n) {
      r(i) = self.values(i) * other.values(i)
      i += 1
    }
    BufferDouble(r)
  }
  def elementwise_mul(other: BufferLong): BufferType = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Double](n)

    while (i < n) {
      r(i) = self.values(i) * other.values(i)
      i += 1
    }
    BufferDouble(r)
  }
  def elementwise_add(other: BufferType): BufferType = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Double](n)

    while (i < n) {
      r(i) = self.values(i) + other.values(i)
      i += 1
    }
    BufferDouble(r)
  }
  def elementwise_add(other: BufferLong): BufferType = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Double](n)

    while (i < n) {
      r(i) = self.values(i) + other.values(i)
      i += 1
    }
    BufferDouble(r)
  }
  def elementwise_subtract(other: BufferType): BufferType = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Double](n)

    while (i < n) {
      r(i) = self.values(i) - other.values(i)
      i += 1
    }
    BufferDouble(r)
  }
  def elementwise_containedIn(other: Set[Double]): BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i)) BufferInt.MissingValue
        else if (other.contains(self.values(i))) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_isMissing: BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) = if (isMissing(i)) 1 else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_abs: BufferDouble = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Double](n)

    while (i < n) {
      r(i) = math.abs(values(i))
      i += 1
    }
    BufferDouble(r)
  }
  def elementwise_roundToDouble: BufferDouble = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Double](n)

    while (i < n) {
      r(i) = math.round(values(i)).toDouble
      i += 1
    }
    BufferDouble(r)
  }
  def elementwise_roundToInt: BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i)) BufferInt.MissingValue
        else math.round(values(i)).toInt
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_printf(s: String): BufferString = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[CharSequence](n)

    while (i < n) {
      r(i) = s.formatted(values(i))
      i += 1
    }
    BufferString(r)
  }
}
