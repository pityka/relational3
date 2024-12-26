package ra3.bufferimpl
import ra3.*
import cats.effect.IO
import java.nio.ByteBuffer
import java.nio.ByteOrder
import tasks.{TaskSystemComponents, SharedFile}
import ra3.join.*
import ra3.join.locator.*
private[ra3] trait BufferIntArrayImpl { self: BufferIntInArray =>

  def nonMissingMinMax = makeStatistic().nonMissingMinMax
  def makeStatistic() = {
    var i = 0
    val n = length
    var hasMissing = false
    var countNonMissing = 0
    var min = Int.MaxValue
    var max = Int.MinValue
    val set = scala.collection.mutable.Set.empty[Int]
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
    StatisticInt(
      hasMissing = hasMissing,
      nonMissingMinMax = if (countNonMissing > 0) Some((min, max)) else None,
      lowCardinalityNonMissingSet =
        if (set.size <= 255) Some(set.toSet) else None
    )
  }

  def elementAsCharSequence(i: Int): CharSequence =
    if (isMissing(i)) "NA" else values(i).toString

  def partition(numPartitions: Int, map: BufferInt): Vector[BufferType] = {
    assert(length == map.length)
    val growableBuffers =
      Vector.fill(numPartitions)(MutableBuffer.emptyI)
    var i = 0
    val n = length
    val mapv = map.values
    while (i < n) {
      growableBuffers(mapv(i)).+=(values(i))
      i += 1
    }
    growableBuffers.map(v => BufferInt(v.toArray))
  }

  def broadcast(n: Int): BufferInt = self.length match {
    case x if x == n => this
    case 1 => BufferIntConstant.apply(value = self.values(0), length = n)
    case _ =>
      throw new RuntimeException("broadcast called on buffer with wrong size")
  }

  def positiveLocations: BufferInt = {
    BufferInt(
      ArrayUtil.findI(values, _ > 0)
    )
  }

  override def toString =
    s"BufferInt(n=${values.length}: ${values.take(5).mkString(", ")} ..})"

  /* Returns a buffer of numGroups. It may overflow. */
  def sumGroups(partitionMap: BufferInt, numGroups: Int): BufferType = {
    assert(partitionMap.length == length)
    val ar = Array.fill[Int](numGroups)(Int.MinValue)
    var i = 0
    val n = partitionMap.length
    while (i < n) {
      if (!isMissing(i)) {
        if (ar(partitionMap.raw(i)) == Int.MinValue) {
          ar(partitionMap.raw(i)) = values(i)
        } else { ar(partitionMap.raw(i)) += values(i) }

      }
      i += 1
    }
    BufferInt(ar)

  }

  def firstInGroup(partitionMap: BufferInt, numGroups: Int): BufferType = {
    assert(partitionMap.length == length)
    val ar = Array.fill(numGroups)(Int.MinValue)
    var i = 0
    val n = partitionMap.length
    while (i < n) {
      if (!isMissing(i)) { ar(partitionMap.raw(i)) = values(i) }
      i += 1
    }
    tag.makeBuffer(ar)

  }

  /** Find locations at which _ <= other[0] or _ >= other[0] holds returns
    * indexes
    */
  def findInequalityVsHead(
      other: BufferType,
      lessThan: Boolean
  ): BufferInt = {
    val c = other.raw(0)
    if (c == Int.MinValue) BufferInt.empty
    else {
      val idx =
        if (lessThan)
          ArrayUtil.findI(values, _ <= c)
        else ArrayUtil.findI(values, _ >= c)

      BufferInt(idx.toArray)
    }
  }

  def toSeq = values.toSeq

  def element(i: Int) = values(i)

  def length = values.length

  def groups = {
    val (counts, uniqueIdx) = ra3.hashtable.IntTable
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

  def mergeNonMissing(
      other: BufferType
  ): BufferType = {
    assert(values.length == other.length)
    var i = 0
    val r = Array.ofDim[Int](values.length)
    while (i < values.length) {
      r(i) = values(i)
      if (isMissing(i)) {
        r(i) = other.raw(i)
      }
      i += 1
    }
    BufferInt(r)

  }

  def computeJoinIndexes(
      other: BufferType,
      how: String
  ): (Option[BufferInt], Option[BufferInt]) = {
    val idx1 = LocatorInt.fromKeys(values)
    val idx2 = LocatorInt.fromKeys(other.values)
    val reindexer = (ra3.join.JoinerImplInt).join(
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

  /** Returns an array of indices */
  def where(i: Int): BufferInt = {
    BufferInt(ArrayUtil.findI(values, _ == i))
  }

  def take(locs: Location): BufferInt = locs match {
    case Slice(start, until) =>
      val r = Array.ofDim[Int](until - start)
      System.arraycopy(values, start, r, 0, until - start)
      BufferInt(r)
    case idx: BufferInt =>
      BufferInt(ArrayUtil.takeI(values, idx.values).toArray)
  }

  override def isMissing(l: Int): Boolean = {
    values(l) == Int.MinValue
  }
  override def hashOf(l: Int): Long = {
    scala.util.hashing.byteswap32(values(l)).toLong
  }

  def toSegment(
      name: LogicalPath
  )(implicit tsc: TaskSystemComponents): IO[SegmentInt] = {
    if (values.length == 0) IO.pure(SegmentInt(None, 0, StatisticInt.empty))
    else
      IO {
        val bb =
          ByteBuffer.allocate(4 * values.length).order(ByteOrder.LITTLE_ENDIAN)
        bb.asIntBuffer().put(values)
        fs2.Stream.chunk(fs2.Chunk.byteBuffer(Utils.compress(bb)))
      }.flatMap { stream =>
        SharedFile
          .apply(stream, name.toString)
          .map(sf => SegmentInt(Some(sf), values.length, self.makeStatistic()))
      }.logElapsed
  }

  def elementwise_*(other: BufferDouble): BufferDouble = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Double](n)
    while (i < n) {
      r(i) =
        if (isMissing(i) || other.isMissing(i)) BufferDouble.MissingValue
        else self.values(i) * other.values(i)
      i += 1
    }
    BufferDouble(r)
  }
  def elementwise_*(other: BufferInt): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)
    while (i < n) {
      r(i) =
        if (isMissing(i) || other.isMissing(i)) BufferInt.MissingValue
        else self.values(i) * other.values(i)
      i += 1
    }
    BufferInt(r)
  }

  def elementwise_+(other: BufferDouble): BufferDouble = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Double](n)
    while (i < n) {
      r(i) =
        if (isMissing(i) || other.isMissing(i)) BufferDouble.MissingValue
        else self.values(i) + other.values(i)
      i += 1
    }
    BufferDouble(r)
  }
  def elementwise_+(other: BufferInt): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)
    while (i < n) {
      r(i) =
        if (isMissing(i) || other.isMissing(i)) BufferInt.MissingValue
        else self.values(i) + other.values(i)
      i += 1
    }
    BufferInt(r)
  }

  def elementwise_&&(other: BufferType): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (self.values(i) > 0 && other.raw(i) > 0) 1
        else if (!isMissing(i) && self.values(i) <= 0) 0
        else if (!other.isMissing(i) && other.values(i) <= 0) 0
        else BufferInt.MissingValue
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_||(other: BufferType): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (self.values(i) > 0 || other.raw(i) > 0) 1
        else if (
          !isMissing(i) && !other.isMissing(i) && self.values(i) <= 0 && other
            .values(i) <= 0
        ) 0
        else BufferInt.MissingValue
      i += 1
    }
    BufferInt(r)
  }

  def elementwise_eq(other: BufferType): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || other.isMissing(i)) BufferInt.MissingValue
        else if (self.values(i) == other.raw(i)) 1
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
        else if (self.values(i) > other.raw(i)) 1
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
        else if (self.values(i) >= other.raw(i)) 1
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
        else if (self.values(i) < other.raw(i)) 1
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
        else if (self.values(i) <= other.raw(i)) 1
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
        else if (self.values(i) != other.raw(i)) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }

  def elementwise_eq(other: BufferDouble): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || other.isMissing(i)) BufferInt.MissingValue
        else if (
          self
            .values(i) == other.values(i).toInt
        ) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_gt(other: BufferDouble): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || other.isMissing(i)) BufferInt.MissingValue
        else if (
          self
            .values(i) > other.values(i).toInt
        ) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_gteq(other: BufferDouble): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || other.isMissing(i)) BufferInt.MissingValue
        else if (
          self
            .values(i) >= other.values(i).toInt
        ) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_lt(other: BufferDouble): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || other.isMissing(i)) BufferInt.MissingValue
        else if (
          self
            .values(i) < other.values(i).toInt
        ) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_lteq(other: BufferDouble): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || other.isMissing(i)) BufferInt.MissingValue
        else if (
          self
            .values(i) <= other.values(i).toInt
        ) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_neq(other: BufferDouble): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || other.isMissing(i)) BufferInt.MissingValue
        else if (
          self
            .values(i) != other.values(i).toInt
        ) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }

  def elementwise_abs: BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i)) BufferInt.MissingValue else math.abs(self.values(i))
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_not: BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i)) BufferInt.MissingValue
        else if (self.values(i) > 0) 0
        else 1
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_toDouble: BufferDouble = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Double](n)

    while (i < n) {
      r(i) =
        if (isMissing(i)) BufferDouble.MissingValue else self.values(i).toDouble
      i += 1
    }
    BufferDouble(r)
  }
  def elementwise_toLong: BufferLong = {
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

  //

  def minInGroups(partitionMap: BufferInt, numGroups: Int): BufferType = {
    val ar = Array.fill[Int](numGroups)(Int.MaxValue)
    var i = 0
    val n = partitionMap.length
    while (i < n) {
      if (!isMissing(i)) {
        val g = partitionMap.raw(i)
        if (ar(g) == Int.MaxValue) {
          ar(g) = values(i)
        } else if (ar(g) > values(i)) {
          ar(g) = values(i)
        }
      }
      i += 1
    }
    BufferInt(ar)

  }
  def maxInGroups(partitionMap: BufferInt, numGroups: Int): BufferType = {
    val ar = Array.fill[Int](numGroups)(Int.MinValue)
    var i = 0
    val n = partitionMap.length
    while (i < n) {
      if (!isMissing(i)) {
        val g = partitionMap.raw(i)
        if (ar(g) == Int.MinValue) {
          ar(g) = values(i)
        } else if (ar(g) < values(i)) {
          ar(g) = values(i)
        }
      }
      i += 1
    }
    BufferInt(ar)

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
  def allInGroups(partitionMap: BufferInt, numGroups: Int): BufferInt = {
    val ar = Array.fill[Int](numGroups)(1)
    var i = 0
    val n = partitionMap.length
    while (i < n) {
      val g = partitionMap.raw(i)
      if (ar(g) == 1 && (isMissing(i) || values(i) == 0)) {
        ar(g) = 0
      }
      i += 1
    }
    BufferInt(ar)

  }
  def anyInGroups(partitionMap: BufferInt, numGroups: Int): BufferInt = {
    val ar = Array.fill[Int](numGroups)(0)
    var i = 0
    val n = partitionMap.length
    while (i < n) {
      val g = partitionMap.raw(i)
      if (ar(g) == 0 && values(i) == 1) {
        ar(g) = 1
      }
      i += 1
    }
    BufferInt(ar)

  }
  def noneInGroups(partitionMap: BufferInt, numGroups: Int): BufferInt = {
    val ar = Array.fill[Int](numGroups)(1)
    var i = 0
    val n = partitionMap.length
    while (i < n) {
      val g = partitionMap.raw(i)
      if (ar(g) == 1 && values(i) == 1) {
        ar(g) = 0
      }
      i += 1
    }
    BufferInt(ar)

  }
  def countInGroups(partitionMap: BufferInt, numGroups: Int): BufferType = {
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
    BufferInt(ar.map(_.toInt))

  }
  def countDistinctInGroups(
      partitionMap: BufferInt,
      numGroups: Int
  ): BufferInt = {
    val ar = Array.fill[scala.collection.mutable.Set[Int]](numGroups)(
      scala.collection.mutable.Set.empty[Int]
    )
    var i = 0
    val n = partitionMap.length
    while (i < n) {
      ar(partitionMap.raw(i)).add(values(i))

      i += 1
    }
    BufferInt(ar.map(_.size))

  }
  def meanInGroups(partitionMap: BufferInt, numGroups: Int): BufferDouble = {
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

  def elementwise_containedIn(set: Set[Int]): ra3.BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i)) BufferInt.MissingValue
        else if (set.contains(self.values(i))) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }

  def elementwise_eq(other: Int): ra3.BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || other == BufferInt.MissingValue)
          BufferInt.MissingValue
        else if (!isMissing(i) && self.values(i) == other) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }

  def elementwise_gt(other: Int): ra3.BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || other == BufferInt.MissingValue)
          BufferInt.MissingValue
        else if (self.values(i) > other) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }

  def elementwise_gteq(other: Int): ra3.BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || other == BufferInt.MissingValue)
          BufferInt.MissingValue
        else if (self.values(i) >= other) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }

  def elementwise_isMissing: ra3.BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) = if (isMissing(i)) 1 else 0
      i += 1
    }
    BufferInt(r)
  }

  def elementwise_lt(other: Int): ra3.BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || other == BufferInt.MissingValue)
          BufferInt.MissingValue
        else if (self.values(i) < other) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }

  def elementwise_lteq(other: Int): ra3.BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || other == BufferInt.MissingValue)
          BufferInt.MissingValue
        else if (self.values(i) <= other) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }

  def elementwise_neq(other: Int): ra3.BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || other == BufferInt.MissingValue)
          BufferInt.MissingValue
        else if (self.values(i) != other) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }

  def elementwise_choose(t: BufferInt, f: BufferInt): BufferInt = {
    assert(t.length == length)
    assert(f.length == length)

    val n = self.length
    val r = Array.ofDim[Int](n)
    var i = 0
    while (i < n) {
      r(i) =
        if (isMissing(i))
          BufferInt.MissingValue
        else if (raw(i) > 0) t.raw(i)
        else f.raw(i)
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_choose(t: BufferInt, f: Int): BufferInt = {
    assert(t.length == length)

    val n = self.length
    val r = Array.ofDim[Int](n)
    var i = 0
    while (i < n) {
      r(i) =
        if (isMissing(i))
          BufferInt.MissingValue
        else if (raw(i) > 0) t.raw(i)
        else f
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_choose(t: Int, f: Int): BufferInt = {

    val n = self.length
    val r = Array.ofDim[Int](n)
    var i = 0
    while (i < n) {
      r(i) =
        if (isMissing(i))
          BufferInt.MissingValue
        else if (raw(i) > 0) t
        else f
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_choose(t: Int, f: BufferInt): BufferInt = {
    assert(f.length == length)

    val n = self.length
    val r = Array.ofDim[Int](n)
    var i = 0
    while (i < n) {
      r(i) =
        if (isMissing(i))
          BufferInt.MissingValue
        else if (raw(i) > 0) t
        else f.raw(i)
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_choose(t: BufferDouble, f: BufferDouble): BufferDouble = {
    assert(t.length == length)
    assert(f.length == length)

    val n = self.length
    val r = Array.ofDim[Double](n)
    var i = 0
    while (i < n) {
      r(i) =
        if (isMissing(i))
          BufferDouble.MissingValue
        else if (raw(i) > 0) t.values(i)
        else f.values(i)
      i += 1
    }
    BufferDouble(r)
  }
  def elementwise_choose(t: Double, f: BufferDouble): BufferDouble = {
    assert(f.length == length)

    val n = self.length
    val r = Array.ofDim[Double](n)
    var i = 0
    while (i < n) {
      r(i) =
        if (isMissing(i))
          BufferDouble.MissingValue
        else if (raw(i) > 0) t
        else f.values(i)
      i += 1
    }
    BufferDouble(r)
  }
  def elementwise_choose(t: BufferDouble, f: Double): BufferDouble = {
    assert(t.length == length)

    val n = self.length
    val r = Array.ofDim[Double](n)
    var i = 0
    while (i < n) {
      r(i) =
        if (isMissing(i))
          BufferDouble.MissingValue
        else if (raw(i) > 0) t.values(i)
        else f
      i += 1
    }
    BufferDouble(r)
  }
  def elementwise_choose(t: Double, f: Double): BufferDouble = {
    val n = self.length
    val r = Array.ofDim[Double](n)
    var i = 0
    while (i < n) {
      r(i) =
        if (isMissing(i))
          BufferDouble.MissingValue
        else if (raw(i) > 0) t
        else f
      i += 1
    }
    BufferDouble(r)
  }

  def elementwise_choose(t: BufferLong, f: BufferLong): BufferLong = {
    assert(t.length == length)
    assert(f.length == length)

    val n = self.length
    val r = Array.ofDim[Long](n)
    var i = 0
    while (i < n) {
      r(i) =
        if (isMissing(i))
          BufferLong.MissingValue
        else if (raw(i) > 0) t.values(i)
        else f.values(i)
      i += 1
    }
    BufferLong(r)
  }
  def elementwise_choose(t: BufferLong, f: Long): BufferLong = {
    assert(t.length == length)

    val n = self.length
    val r = Array.ofDim[Long](n)
    var i = 0
    while (i < n) {
      r(i) =
        if (isMissing(i))
          BufferLong.MissingValue
        else if (raw(i) > 0) t.values(i)
        else f
      i += 1
    }
    BufferLong(r)
  }
  def elementwise_choose(t: Long, f: BufferLong): BufferLong = {
    assert(f.length == length)

    val n = self.length
    val r = Array.ofDim[Long](n)
    var i = 0
    while (i < n) {
      r(i) =
        if (isMissing(i))
          BufferLong.MissingValue
        else if (raw(i) > 0) t
        else f.values(i)
      i += 1
    }
    BufferLong(r)
  }
  def elementwise_choose(t: Long, f: Long): BufferLong = {

    val n = self.length
    val r = Array.ofDim[Long](n)
    var i = 0
    while (i < n) {
      r(i) =
        if (isMissing(i))
          BufferLong.MissingValue
        else if (raw(i) > 0) t
        else f
      i += 1
    }
    BufferLong(r)
  }
  def elementwise_choose(t: BufferInstant, f: BufferInstant): BufferInstant = {
    assert(t.length == length)
    assert(f.length == length)

    val n = self.length
    val r = Array.ofDim[Long](n)
    var i = 0
    while (i < n) {
      r(i) =
        if (isMissing(i))
          BufferInstant.MissingValue
        else if (raw(i) > 0) t.values(i)
        else f.values(i)
      i += 1
    }
    BufferInstant(r)
  }
  def elementwise_choose(t: Long, f: BufferInstant): BufferInstant = {
    assert(f.length == length)

    val n = self.length
    val r = Array.ofDim[Long](n)
    var i = 0
    while (i < n) {
      r(i) =
        if (isMissing(i))
          BufferInstant.MissingValue
        else if (raw(i) > 0) t
        else f.values(i)
      i += 1
    }
    BufferInstant(r)
  }
  def elementwise_choose(t: BufferInstant, f: Long): BufferInstant = {
    assert(t.length == length)

    val n = self.length
    val r = Array.ofDim[Long](n)
    var i = 0
    while (i < n) {
      r(i) =
        if (isMissing(i))
          BufferInstant.MissingValue
        else if (raw(i) > 0) t.values(i)
        else f
      i += 1
    }
    BufferInstant(r)
  }
  def elementwise_choose_inst(t: Long, f: Long): BufferInstant = {

    val n = self.length
    val r = Array.ofDim[Long](n)
    var i = 0
    while (i < n) {
      r(i) =
        if (isMissing(i))
          BufferInstant.MissingValue
        else if (raw(i) > 0) t
        else f
      i += 1
    }
    BufferInstant(r)
  }

  def elementwise_choose(t: BufferString, f: BufferString): BufferString = {
    assert(t.length == length)
    assert(f.length == length)

    val n = self.length
    val r = Array.ofDim[CharSequence](n)
    var i = 0
    while (i < n) {
      r(i) =
        if (isMissing(i))
          BufferString.MissingValue
        else if (raw(i) > 0) t.values(i)
        else f.values(i)
      i += 1
    }
    BufferString(r)
  }

  def elementwise_choose(t: String, f: BufferString): BufferString = {
    assert(f.length == length)

    val n = self.length
    val r = Array.ofDim[CharSequence](n)
    var i = 0
    while (i < n) {
      r(i) =
        if (isMissing(i))
          BufferString.MissingValue
        else if (raw(i) > 0) t
        else f.values(i)
      i += 1
    }
    BufferString(r)
  }
  def elementwise_choose(t: BufferString, f: String): BufferString = {
    assert(t.length == length)

    val n = self.length
    val r = Array.ofDim[CharSequence](n)
    var i = 0
    while (i < n) {
      r(i) =
        if (isMissing(i))
          BufferString.MissingValue
        else if (raw(i) > 0) t.values(i)
        else f
      i += 1
    }
    BufferString(r)
  }
  def elementwise_choose(t: String, f: String): BufferString = {

    val n = self.length
    val r = Array.ofDim[CharSequence](n)
    var i = 0
    while (i < n) {
      r(i) =
        if (isMissing(i))
          BufferString.MissingValue
        else if (raw(i) > 0) t
        else f
      i += 1
    }
    BufferString(r)
  }
}
