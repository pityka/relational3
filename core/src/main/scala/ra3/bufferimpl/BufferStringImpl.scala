package ra3.bufferimpl
import ra3.*
import cats.effect.IO
import java.nio.ByteBuffer
import java.nio.ByteOrder
import tasks.{TaskSystemComponents, SharedFile}
import ra3.join.*
import ra3.join.locator.*
import org.saddle.scalar.{ScalarTagInt, ScalarTagDouble, ScalarTagLong}

private[ra3] object CharSequenceOrdering
    extends scala.math.Ordering[CharSequence] { self =>

  val joiner = (new ra3.join.JoinerImplCharSequence(v => isMissing(v)))
  def charSeqEquals(x: CharSequence, y: CharSequence) = {
    if (x.length() != y.length) false
    else {
      var i = 0
      val n = x.length
      var b = true
      while (i < n && b) {
        b = (x.charAt(i) == y.charAt(i))

        i += 1
      }
      b
    }
  }

  private val missing = BufferString.missing
  def isMissing(x: CharSequence) = {
    if (x.length() != missing.length) false
    else {
      var i = 0
      val n = x.length
      var b = true
      while (i < n && b) {
        b = (x.charAt(i) == missing.charAt(i))

        i += 1
      }
      b
    }
  }

  def compare(x: CharSequence, y: CharSequence): Int =
    CharSequence.compare(x, y)

}

private[ra3] trait BufferStringImpl { self: BufferString =>
  def nonMissingMinMax = makeStatistic().nonMissingMinMax

  def makeStatistic() = {
    var i = 0
    val n = length
    var hasMissing = false
    var countNonMissing = 0
    var min = Option.empty[CharSequence]
    var max = Option.empty[CharSequence]
    val set = scala.collection.mutable.Set.empty[CharSequence]
    val setBF = scala.collection.mutable.Set.empty[CharSequence]
    while (i < n) {
      if (isMissing(i)) {
        hasMissing = true
      } else {
        countNonMissing += 1
        val v = values(i)
        if (min.isEmpty || CharSequenceOrdering.lt(v, min.get)) {
          min = Some(v)
        }
        if (max.isEmpty || CharSequenceOrdering.gt(v, max.get)) {
          max = Some(v)
        }
        if (set.size < 256 && !set.contains(v)) {
          set.+=(v)
        }
        if (setBF.size < 16384 && !setBF.contains(v)) {
          setBF.add(v)
        }
      }
      i += 1
    }
    val bloomFilter = BloomFilter.makeFromCharSequence(4096, 2, setBF)

    StatisticCharSequence(
      hasMissing = hasMissing,
      nonMissingMinMax =
        if (countNonMissing > 0) Some((min.get, max.get)) else None,
      lowCardinalityNonMissingSet =
        if (set.size <= 255) Some(set.toSet) else None,
      bloomFilter = Some(bloomFilter)
    )
  }

  def elementAsCharSequence(i: Int): CharSequence =
    if (isMissing(i)) "NA" else values(i)

  def partition(numPartitions: Int, map: BufferInt): Vector[BufferType] = {
    assert(length == map.length)
    val growableBuffers =
      Vector.fill(numPartitions)(MutableBuffer.emptyG[CharSequence])
    var i = 0
    val n = length
    val mapv = map.values
    while (i < n) {
      growableBuffers(mapv(i)).+=(values(i))
      i += 1
    }
    growableBuffers.map(v => BufferString(v.toArray))
  }

  def broadcast(n: Int) = self.length match {
    case x if x == n => this
    case 1 =>
      BufferString(Array.fill[CharSequence](n)(values(0)))
    case _ =>
      throw new RuntimeException("broadcast called on buffer with wrong size")
  }

  def positiveLocations: BufferInt = {
    BufferInt(
      ArrayUtil.findG(values, s => s != BufferString.missing && s.length > 0)
    )
  }

  override def toString =
    s"BufferString(n=${values.length}: ${values.take(5).mkString(", ")} ..})"

  /** Find locations at which _ <= other[0] or _ >= other[0] holds returns
    * indexes
    */
  def findInequalityVsHead(
      other: BufferType,
      lessThan: Boolean
  ): BufferInt = {
    val ord = CharSequenceOrdering
    val c = other.values(0)
    if (ord.isMissing(c)) BufferInt.empty
    else {
      val idx =
        if (lessThan)
          ArrayUtil.findG(values, x => !ord.isMissing(x) && ord.lteq(x, c))
        else ArrayUtil.findG(values, x => !ord.isMissing(x) && ord.gteq(x, c))

      BufferInt(idx.toArray)
    }
  }

  def toSeq = values.toSeq

  def element(i: Int) = values(i)

  def cdf(numPoints: Int): (BufferString, BufferDouble) = {
    val percentiles =
      ((0 until (numPoints - 1)).map(i => i * (1d / (numPoints - 1))) ++ List(
        1d
      )).distinct
    val sorted =
      values
        .filterNot(_ == BufferString.MissingValue)
        .sorted(CharSequenceOrdering)
    val cdf = percentiles.map { p =>
      val idx = (p * (sorted.length - 1)).toInt
      (sorted(idx), p)
    }

    val x = BufferString(cdf.map(_._1).toArray)
    val y = BufferDouble(cdf.map(_._2).toArray)
    (x, y)
  }

  def length = values.length

  def groups = {

    val (counts, uniqueIdx) = ra3.hashtable.CharSequenceTable
      .buildWithUniques(values, Array.ofDim[Int](values.length))

    val map = Array.ofDim[Int](values.length)
    var i = 0
    while (i < values.length) {
      counts.mutate(values(i), _ + 1)
      i += 1
    }
    i = 0
    val groups = uniqueIdx.map(values)
    val groupmap = ra3.hashtable.CharSequenceTable.buildFirst(groups, null)
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
    val otherValues = other.values
    assert(values.length == otherValues.length)
    var i = 0
    val r = Array.ofDim[CharSequence](values.length)
    while (i < values.length) {
      r(i) = values(i)
      if (isMissing(i)) {
        r(i) = otherValues(i)
      }
      i += 1
    }
    BufferString(r)

  }

  def computeJoinIndexes(
      other: BufferType,
      how: String
  ): (Option[BufferInt], Option[BufferInt]) = {

    val idx1 = LocatorCharSequence.fromKeys(values)
    val idx2 = LocatorCharSequence(other.values)
    val reindexer = CharSequenceOrdering.joiner.join(
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

  def take(locs: Location): BufferString = locs match {
    case Slice(start, until) =>
      val r = Array.ofDim[CharSequence](until - start)
      System.arraycopy(values, start, r, 0, until - start)
      BufferString(r)
    case idx: BufferInt =>
      val ar = Array.ofDim[CharSequence](idx.length)
      var i = 0
      val n = ar.length
      val v = idx.values
      while (i < n) {
        val t = v(i)
        ar(i) = if (t < 0) BufferString.missing else values(t)
        i += 1
      }
      BufferString(
        ar
      )
  }

  override def isMissing(l: Int): Boolean = {
    CharSequenceOrdering.isMissing(values(l))
  }
  private val hasher = scala.util.hashing.MurmurHash3.arrayHashing[Int]
  override def hashOf(l: Int): Long = {
    hasher.hash(values(l).codePoints.toArray).toLong
  }

  def firstInGroup(partitionMap: BufferInt, numGroups: Int): BufferType = {
    assert(partitionMap.length == length)
    val ar: Array[CharSequence] = Array.fill(numGroups)(s"${Char.MinValue}")
    var i = 0
    val n = partitionMap.length
    while (i < n) {
      if (!isMissing(i)) { ar(partitionMap.raw(i)) = values(i) }
      i += 1
    }
    tag.makeBuffer(ar)

  }

  def toSegment(
      name: LogicalPath
  )(implicit tsc: TaskSystemComponents): IO[SegmentString] = {
    if (values.length == 0)
      IO.pure(SegmentString(None, 0, 0L, StatisticCharSequence.empty))
    else
      IO {
        val byteSize = values.map(v => (v.length * 2L) + 4).sum
        if (byteSize > Int.MaxValue - 100)
          (
            fs2.Stream.raiseError[IO](
              new RuntimeException(
                "String buffers longer than what fits into an array not implemented"
              )
            ),
            -1L
          )
        else {
          val t1 = System.nanoTime()
          val bb =
            ByteBuffer.allocate(byteSize.toInt).order(ByteOrder.BIG_ENDIAN)
          var numBytes = 0L
          values.foreach { str =>

            numBytes += str.length() * 2 + 40
            bb.putInt(str.length)
            var i = 0
            while (i < str.length) {
              bb.putChar(str.charAt(i))
              i += 1
            }

          }
          bb.flip()

          val bbCompressed = Utils.compress(bb)
          val t2 = System.nanoTime()

          (fs2.Stream.chunk(fs2.Chunk.byteBuffer(bbCompressed)), numBytes)
        }
      }.flatMap { case (stream, numBytes) =>
        SharedFile
          .apply(stream, name.toString)
          .map(sf =>
            SegmentString(
              Some(sf),
              values.length,
              numBytes,
              self.makeStatistic()
            )
          )
      }
      .logElapsed
  }

  def elementwise_+(other: BufferType): BufferType = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[CharSequence](n)
    while (i < n) {
      r(i) =
        if (isMissing(i) || other.isMissing(i)) BufferString.MissingValue
        else self.values(i).toString + other.values(i).toString()
      i += 1
    }
    BufferString(r)
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
          CharSequence.compare(
            self.values(i),
            other.values(i)
          ) == 0
        ) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_eq(other: String): BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)
    while (i < n) {
      r(i) =
        if (isMissing(i) || CharSequenceOrdering.isMissing(other))
          BufferInt.MissingValue
        else if (
          CharSequence
            .compare(self.values(i), other) == 0
        ) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_containedIn(other: Set[String]): BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i)) BufferInt.MissingValue
        else if (
          other
            .exists(b => CharSequence.compare(self.values(i), b) == 0)
        ) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_matches(other: String): BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)
    val pattern = other.r
    while (i < n) {
      r(i) =
        if (isMissing(i) || CharSequenceOrdering.isMissing(other))
          BufferInt.MissingValue
        else if (pattern.matches(values(i))) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_concatenate(other: String): BufferString = {

    var i = 0
    val n = self.length
    val r = Array.ofDim[CharSequence](n)
    while (i < n) {
      r(i) =
        if (CharSequenceOrdering.isMissing(other) || isMissing(i))
          BufferString.MissingValue
        else (values(i).toString + other)
      i += 1
    }
    BufferString(r)
  }

  def elementwise_concatenate(other: BufferString): BufferString = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[CharSequence](n)
    while (i < n) {
      r(i) =
        if (isMissing(i) || other.isMissing(i)) BufferString.MissingValue
        else (values(i).toString + other.values(i).toString)
      i += 1
    }
    BufferString(r)
  }
  def elementwise_gt(other: BufferType): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || other.isMissing(i)) BufferInt.MissingValue
        else if (
          CharSequenceOrdering.gt(
            self.values(i),
            other.values(i)
          )
        ) 1
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
        else if (
          CharSequenceOrdering.gteq(
            self.values(i),
            other.values(i)
          )
        ) 1
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
        else if (
          CharSequenceOrdering.lt(
            self.values(i),
            other.values(i)
          )
        ) 1
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
        else if (
          CharSequenceOrdering.lteq(
            self.values(i),
            other.values(i)
          )
        ) 1
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
        else if (
          CharSequence.compare(
            self.values(i),
            other.values(i)
          ) == 0
        ) 0
        else 1
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_neq(other: String): BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || CharSequenceOrdering.isMissing(other))
          BufferInt.MissingValue
        else if (
          CharSequence
            .compare(self.values(i), other) == 0
        ) 0
        else 1
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_length: BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)
    while (i < n) {
      r(i) =
        if (isMissing(i)) BufferInt.MissingValue else self.values(i).length()
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_nonempty: BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)
    while (i < n) {
      r(i) = if (!isMissing(i) && self.values(i).length() > 0) 1 else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_parseInt: BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)
    def parse(cs: CharSequence) = {
      cs match {
        case ra3.CharArraySubSeq(buf, start, to) =>
          ScalarTagInt.parse(buf, start, to - start)
        case _ =>
          val ar = Array.ofDim[Char](cs.length())
          var i = 0
          while (i < cs.length()) {
            ar(i) = cs.charAt(i)
            i += 1
          }
          ScalarTagInt.parse(ar, 0, cs.length())
      }
    }
    while (i < n) {
      r(i) = if (isMissing(i)) BufferInt.MissingValue else parse(self.values(i))
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_parseDouble: BufferDouble = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Double](n)
    def parse(cs: CharSequence) = {
      cs match {
        case ra3.CharArraySubSeq(buf, start, to) =>
          ScalarTagDouble.parse(buf, start, to - start)
        case _ =>
          val ar = Array.ofDim[Char](cs.length())
          var i = 0
          while (i < cs.length()) {
            ar(i) = cs.charAt(i)
            i += 1
          }
          ScalarTagDouble.parse(ar, 0, cs.length())
      }
    }
    while (i < n) {
      r(i) = if (isMissing(i)) BufferInt.MissingValue else parse(self.values(i))
      i += 1
    }
    BufferDouble(r)
  }
  def elementwise_parseLong: BufferLong = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Long](n)
    def parse(cs: CharSequence) = {
      cs match {
        case ra3.CharArraySubSeq(buf, start, to) =>
          ScalarTagLong.parse(buf, start, to - start)
        case _ =>
          val ar = Array.ofDim[Char](cs.length())
          var i = 0
          while (i < cs.length()) {
            ar(i) = cs.charAt(i)
            i += 1
          }
          ScalarTagLong.parse(ar, 0, cs.length())
      }

    }
    while (i < n) {
      r(i) = if (isMissing(i)) BufferInt.MissingValue else parse(self.values(i))
      i += 1
    }
    BufferLong(r)
  }
  def elementwise_parseInstant: BufferInstant = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Long](n)

    while (i < n) {
      r(i) =
        if (isMissing(i)) BufferInstant.MissingValue
        else java.time.Instant.parse(self.values(i)).toEpochMilli()
      i += 1
    }
    BufferInstant(r)
  }
  def elementwise_substring(start: Int, len: Int): BufferString = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[CharSequence](n)

    while (i < n) {
      r(i) =
        if (isMissing(i)) BufferString.MissingValue
        else values(i).subSequence(start, start + len)
      i += 1
    }
    BufferString(r)
  }

  def elementwise_matches_replace(
      pattern: String,
      replacement: String
  ): BufferString = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[CharSequence](n)
    val pattern1 = pattern.r
    while (i < n) {
      r(i) =
        if (isMissing(i)) BufferString.MissingValue
        else if (pattern1.matches(values(i))) replacement
        else values(i)
      i += 1
    }
    BufferString(r)
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
    val ar = Array.fill[scala.collection.mutable.Set[String]](numGroups)(
      scala.collection.mutable.Set.empty[String]
    )
    var i = 0
    val n = partitionMap.length
    while (i < n) {
      ar(partitionMap.raw(i)).add(values(i).toString)

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

  def elementwise_lteq(other: String): BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || CharSequenceOrdering.isMissing(other))
          BufferInt.MissingValue
        else if (
          CharSequenceOrdering.lteq(
            self.values(i),
            other
          )
        ) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_lt(other: String): BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || CharSequenceOrdering.isMissing(other))
          BufferInt.MissingValue
        else if (
          CharSequenceOrdering.lt(
            self.values(i),
            other
          )
        ) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_gt(other: String): BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || CharSequenceOrdering.isMissing(other))
          BufferInt.MissingValue
        else if (
          CharSequenceOrdering.gt(
            self.values(i),
            other
          )
        ) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_gteq(other: String): BufferInt = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)

    while (i < n) {
      r(i) =
        if (isMissing(i) || CharSequenceOrdering.isMissing(other))
          BufferInt.MissingValue
        else if (
          CharSequenceOrdering.gteq(
            self.values(i),
            other
          )
        ) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }

}
