package ra3.bufferimpl
import ra3._
import cats.effect.IO
import java.nio.ByteBuffer
import java.nio.ByteOrder
import tasks.{TaskSystemComponents, SharedFile}

private[ra3] trait BufferLongImpl { self: BufferLong =>
  def nonMissingMinMax = makeStatistic().nonMissingMinMax

  def makeStatistic() = {
    var i = 0
    val n = length
    var hasMissing = false
    var countNonMissing = 0
    var min = Long.MaxValue
    var max = Long.MinValue
    val set = scala.collection.mutable.Set.empty[Long]
    val setBF = scala.collection.mutable.LongMap.empty[AnyRef]
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
        if (setBF.size < 16384 && !setBF.contains(v)) {
          setBF.update(v, null)
        }
      }
      i += 1
    }
    val bloomFilter = BloomFilter.makeFromLongs(4096, 2, setBF.keySet)
    StatisticLong(
      hasMissing = hasMissing,
      nonMissingMinMax = if (countNonMissing > 0) Some((min, max)) else None,
      lowCardinalityNonMissingSet =
        if (set.size <= 255) Some(set.toSet) else None,
      bloomFilter = Some(bloomFilter)
    )
  }

  def elementAsCharSequence(i: Int): CharSequence =
    if (isMissing(i)) "NA" else values(i).toString

  def partition(numPartitions: Int, map: BufferInt): Vector[BufferType] = {
    assert(length == map.length)
    val growableBuffers =
      Vector.fill(numPartitions)(org.saddle.Buffer.empty[Long])
    var i = 0
    val n = length
    val mapv = map.values
    while (i < n) {
      growableBuffers(mapv(i)).+=(values(i))
      i += 1
    }
    growableBuffers.map(v => BufferLong(v.toArray))
  }

  def broadcast(n: Int) = self.length match {
    case x if x == n => this
    case 1 =>
      BufferLong(Array.fill[Long](n)(values(0)))
    case _ =>
      throw new RuntimeException("broadcast called on buffer with wrong size")
  }

  def positiveLocations: BufferInt = {
    import org.saddle._
    BufferInt(
      values.toVec.find(_ > 0L).toArray
    )
  }

  override def toString =
    s"BufferLong(n=${values.length}: ${values.take(5).mkString(", ")} ..})"

  /* Returns a buffer of numGroups. It may overflow. */
  def sumGroups(partitionMap: BufferInt, numGroups: Int): BufferType = {
    val ar = Array.ofDim[Long](numGroups)
    var i = 0
    val n = partitionMap.length
    while (i < n) {
      if (!isMissing(i)) {
        if (ar(partitionMap.raw(i)) == Long.MinValue) {
          ar(partitionMap.raw(i)) = values(i)
        } else { ar(partitionMap.raw(i)) += values(i) }
      }
      i += 1
    }
    BufferLong(ar)

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
    if (c == BufferLong.MissingValue) BufferInt.empty
    else {
      val idx =
        if (lessThan)
          values.toVec.find(_ <= c)
        else values.toVec.find(_ >= c)

      BufferInt(idx.toArray)
    }
  }

  def toSeq = values.toSeq

  def cdf(numPoints: Int): (BufferLong, BufferDouble) = {
    val percentiles =
      ((0 until (numPoints - 1)).map(i => i * (1d / (numPoints - 1))) ++ List(
        1d
      )).distinct
      import org.saddle._
    val sorted = org.saddle.array.sort[Long](values.toVec.dropNA.toArray)
    val cdf = percentiles.map { p =>
      val idx = (p * (sorted.length - 1)).toInt
      (sorted(idx), p)
    }

    val x = BufferLong(cdf.map(_._1).toArray)
    val y = BufferDouble(cdf.map(_._2).toArray)
    (x, y)
  }

  def length = values.length

  import org.saddle.{Buffer => _, _}

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
    val otherValues = other.values
    assert(values.length == otherValues.length)
    var i = 0
    val r = Array.ofDim[Long](values.length)
    while (i < values.length) {
      r(i) = values(i)
      if (isMissing(i)) {
        r(i) = otherValues(i)
      }
      i += 1
    }
    BufferLong(r)

  }

  def computeJoinIndexes(
      other: BufferType,
      how: String
  ): (Option[BufferInt], Option[BufferInt]) = {
    val idx1 = Index(values)
    val idx2 = Index(other.values)
    val reindexer = new (ra3.join.JoinerImpl[Long]).join(
      left = idx1,
      right = idx2,
      how = how match {
        case "inner" => org.saddle.index.InnerJoin
        case "left"  => org.saddle.index.LeftJoin
        case "right" => org.saddle.index.RightJoin
        case "outer" => org.saddle.index.OuterJoin
      }
    )
    (reindexer.lTake.map(BufferInt(_)), reindexer.rTake.map(BufferInt(_)))
  }

  override def take(locs: Location): BufferLong = locs match {
    case Slice(start, until) =>
      val r = Array.ofDim[Long](until - start)
      System.arraycopy(values, start, r, 0, until - start)
      BufferLong(r)
    case idx: BufferInt =>
      import org.saddle._
      BufferLong(values.toVec.take(idx.values).toArray)
  }

  override def isMissing(l: Int): Boolean = {
    values(l) == Long.MinValue
  }
  override def hashOf(l: Int): Long = {
    scala.util.hashing.byteswap64(values(l))
  }

  def firstInGroup(partitionMap: BufferInt, numGroups: Int): BufferType = {
    assert(partitionMap.length == length)
    val ar = Array.fill(numGroups)(Long.MinValue)
    var i = 0
    val n = partitionMap.length
    while (i < n) {
      if (!isMissing(i)) { ar(partitionMap.raw(i)) = values(i) }
      i += 1
    }
    tag.makeBuffer(ar)

  }
  override def toSegment(
      name: LogicalPath
  )(implicit tsc: TaskSystemComponents): IO[SegmentLong] = {
    if (values.length == 0) IO.pure(SegmentLong(None, 0, StatisticLong.empty))
    else
      IO {
        val bb =
          ByteBuffer.allocate(8 * values.length).order(ByteOrder.LITTLE_ENDIAN)
        bb.asLongBuffer().put(values)
        fs2.Stream.chunk(fs2.Chunk.byteBuffer(Utils.compress(bb)))
      }.flatMap { stream =>
        SharedFile
          .apply(stream, name.toString)
          .map(sf => SegmentLong(Some(sf), values.length, self.makeStatistic()))
      }

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
  def elementwise_toInstantEpochMilli: BufferInstant = {
    val copy = values.clone()
    BufferInstant(copy)
  }

  def countInGroups(partitionMap: BufferInt, numGroups: Int): BufferInt = {
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
    val ar = Array.fill[scala.collection.mutable.Set[Long]](numGroups)(
      scala.collection.mutable.Set.empty[Long]
    )
    var i = 0
    val n = partitionMap.length
    while (i < n) {
      ar(partitionMap.raw(i)).add(values(i))

      i += 1
    }
    BufferInt(ar.map(_.size))

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
  def elementwise_eq(other: BufferType): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || other.isMissing(i)) BufferInt.MissingValue
        else if (
          self.values(i) == other
            .values(i)
        ) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_eq(other: Long): BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || other == BufferLong.MissingValue)
          BufferInt.MissingValue
        else if (
          self.values(
            i
          ) == other
        ) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }

}
