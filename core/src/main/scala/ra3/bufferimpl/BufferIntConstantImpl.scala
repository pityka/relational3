package ra3.bufferimpl
import cats.effect.IO
import ra3._
import tasks.{TaskSystemComponents}
private[ra3] trait BufferIntConstantImpl { self: BufferIntConstant =>
def nonMissingMinMax = makeStatistic().nonMissingMinMax
  def makeStatistic() = {
    
   
    StatisticInt(
      hasMissing = if (value == Int.MinValue) true else false,
      nonMissingMinMax = if (value == Int.MinValue) None else Some((value,value)),
      lowCardinalityNonMissingSet = if (value == Int.MinValue) None else Some(Set(value)),
      countNonMissing = if (value == Int.MinValue) 0 else length
    )
  }

  def elementAsCharSequence(i: Int): CharSequence =
    if (value == Int.MinValue) "NA" else value.toString

  def partition(numPartitions: Int, map: BufferInt): Vector[BufferType] = {
    assert(length == map.length)
    val growableBuffers =
      Vector.fill(numPartitions)(org.saddle.Buffer.empty[Int])
    var i = 0
    val n = length
    val mapv = map.values
    while (i < n) {
      growableBuffers(mapv(i)).+=(value)
      i += 1
    }
    growableBuffers.map(v => BufferInt(v.toArray))
  }

  def broadcast(n: Int) = self.length match {
    case x if x == n => this
    case 1 =>
      BufferIntConstant(value, n)
    case _ =>
      throw new RuntimeException("broadcast called on buffer with wrong size")
  }

  def positiveLocations: BufferInt = {
    import org.saddle._
    if (self.value > 0)
      BufferInt(
        array.range(0, self.length)
      )
    else BufferInt.empty
  }

  override def toString =
    s"BufferIntConstant(n=${length}: ${self.value} ..})"

  /* Returns a buffer of numGroups. It may overflow. */
  def sumGroups(partitionMap: BufferInt, numGroups: Int): BufferType = {
    assert(partitionMap.length == length)
    val ar = Array.fill[Int](numGroups)(Int.MinValue)
    var i = 0
    val n = partitionMap.length
    while (i < n) {
      if (!isMissing(i)) {
        if (ar(partitionMap.raw(i)) == Int.MinValue) {
          ar(partitionMap.raw(i)) = value
        } else { ar(partitionMap.raw(i)) += value }

      }
      i += 1
    }
    BufferInt(ar)

  }

  def firstInGroup(partitionMap: BufferInt, numGroups: Int): BufferType = {
    assert(partitionMap.length == length)
    val ar = Array.fill[Int](numGroups)(value)
    tag.makeBuffer(ar)

  }

  /** Find locations at which _ <= other[0] or _ >= other[0] holds returns
    * indexes
    */
  override def findInequalityVsHead(
      other: BufferType,
      lessThan: Boolean
  ): BufferInt = {
    import org.saddle._
    val c = other.raw(0)
    if (value == Int.MinValue || c == Int.MinValue) BufferInt.empty
    else {
      if (lessThan) {
        if (value <= c) {
          BufferInt(array.range(0, length))
        } else BufferInt.empty
      } else if (value >= c) {
        BufferInt(array.range(0, length))
      } else BufferInt.empty
    }
  }

  def toSeq = values.toSeq

  def cdf(numPoints: Int): (BufferInt, BufferDouble) = {
    val percentiles =
      ((0 until (numPoints - 1)).map(i => i * (1d / (numPoints - 1))) ++ List(
        1d
      )).distinct
    val cdf = percentiles.map { p =>
      (value, p)
    }

    val x = BufferInt(cdf.map(_._1).toArray)
    val y = BufferDouble(cdf.map(_._2).toArray)
    (x, y)
  }

  import org.saddle.{Buffer => _, _}

  def groups = {
    Buffer.GroupMap(
      map = BufferInt(Array.ofDim[Int](length)),
      numGroups = if (length == 0) 0 else 1,
      groupSizes = BufferInt(length)
    )
  }

  def mergeNonMissing(
      other: BufferType
  ): BufferType = {
    assert(self.length == other.length)
    if (length == 0) self
    else if (isMissing(0)) other
    else self

  }

  def computeJoinIndexes(
      other: BufferType,
      how: String
  ): (Option[BufferInt], Option[BufferInt]) = {
    val idx1 = Index(values)
    val idx2 = Index(other.values)
    val reindexer = new (org.saddle.index.JoinerImpl[Int]).join(
      left = idx1,
      right = idx2,
      how = how match {
        case "inner" => org.saddle.index.InnerJoin
        case "left"  => org.saddle.index.LeftJoin
        case "right" => org.saddle.index.RightJoin
        case "outer" => org.saddle.index.OuterJoin
      },
      forceProperSemantics = true
    )
    (reindexer.lTake.map(BufferInt(_)), reindexer.rTake.map(BufferInt(_)))
  }

  /** Returns an array of indices */
  def where(i: Int): BufferInt = {
    if (i == value) BufferInt(array.range(0, length))
    else BufferInt.empty
  }

  override def take(locs: Location): BufferInt = locs match {
    case Slice(start, until) =>
      BufferInt.constant(value, until - start)
    case idx: BufferInt =>
      BufferInt(values.toVec.take(idx.values).toArray)

  }

  override def isMissing(l: Int): Boolean = {
    value == Int.MinValue
  }
  override def hashOf(l: Int): Long = {
    scala.util.hashing.byteswap32(value).toLong
  }

  

  override def toSegment(
      name: LogicalPath
  )(implicit tsc: TaskSystemComponents): IO[SegmentInt] = {
    if (values.length == 0) IO.pure(SegmentInt(None, 0, StatisticInt.empty))
    else
      IO.pure {

        SegmentInt(None, values.length, self.makeStatistic())
      }

  }

  // Elementwise operations

  def elementwise_*(other: BufferDouble): BufferDouble = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Double](n)
    while (i < n) {
      r(i) =
        if (self.value == Int.MinValue || other.isMissing(i))
          BufferDouble.MissingValue
        else self.value.toDouble * other.values(i)
      i += 1
    }
    BufferDouble(r)
  }

  def elementwise_+(other: ra3.BufferDouble): ra3.BufferDouble = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Double](n)
    while (i < n) {
      r(i) =
        if (self.value == Int.MinValue || other.isMissing(i))
          BufferDouble.MissingValue
        else self.value + other.values(i)
      i += 1
    }
    BufferDouble(r)
  }
  def elementwise_+(other: ra3.BufferInt): ra3.BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)
    while (i < n) {
      r(i) =
        if (self.value == Int.MinValue || other.isMissing(i))
          BufferInt.MissingValue
        else self.value + other.values(i)
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_*(other: ra3.BufferInt): ra3.BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)
    while (i < n) {
      r(i) =
        if (self.value == Int.MinValue || other.isMissing(i))
          BufferInt.MissingValue
        else self.value * other.values(i)
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
        if (
          !isMissing(i) && !other
            .isMissing(i) && self.value > 0 && other.raw(i) > 0
        ) 1
        else 0
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
        if (
          !isMissing(i) && !other
            .isMissing(i) && self.value > 0 || other.raw(i) > 0
        ) 1
        else 0
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
        if (!isMissing(i) && !other.isMissing(i) && self.value == other.raw(i))
          1
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
        if (!isMissing(i) && !other.isMissing(i) && self.value > other.raw(i)) 1
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
        if (!isMissing(i) && !other.isMissing(i) && self.value >= other.raw(i))
          1
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
        if (!isMissing(i) && !other.isMissing(i) && self.value < other.raw(i)) 1
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
        if (!isMissing(i) && !other.isMissing(i) && self.value <= other.raw(i))
          1
        else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_lteq(other: Double): BufferInt = {

    BufferInt.constant(
      if (!isMissing(0) && self.value <= other) 1 else 0,
      length
    )
  }
  def elementwise_neq(other: BufferType): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)
    while (i < n) {
      r(i) =
        if (!isMissing(i) && !other.isMissing(i) && self.value != other.raw(i))
          1
        else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_neq(other: Double): BufferInt = {
    BufferInt.constant(
      if (!isMissing(0) && self.value != other) 1 else 0,
      length
    )
  }

  def elementwise_eq(other: BufferDouble): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)
    while (i < n) {
      r(i) = if (!isMissing(i) && self.value == other.values(i)) 1 else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_eq(other: Double): BufferInt = {
    BufferInt.constant(
      if (!isMissing(0) && self.value == other) 1 else 0,
      length
    )
  }
  def elementwise_gt(other: BufferDouble): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)
    while (i < n) {
      r(i) = if (!isMissing(i) && self.value > other.values(i)) 1 else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_gt(other: Double): BufferInt = {
    BufferInt.constant(
      if (!isMissing(0) && self.value > other) 1 else 0,
      length
    )
  }
  def elementwise_gteq(other: BufferDouble): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)
    while (i < n) {
      r(i) = if (!isMissing(i) && self.value >= other.values(i)) 1 else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_gteq(other: Double): BufferInt = {
    BufferInt.constant(
      if (!isMissing(0) && self.value >= other) 1 else 0,
      length
    )
  }
  def elementwise_lt(other: BufferDouble): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)
    while (i < n) {
      r(i) = if (!isMissing(i) && self.value < other.values(i)) 1 else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_lt(other: Double): BufferInt = {
    BufferInt.constant(
      if (!isMissing(0) && self.value < other) 1 else 0,
      length
    )
  }
  def elementwise_lteq(other: BufferDouble): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)
    while (i < n) {
      r(i) = if (!isMissing(i) && self.value <= other.values(i)) 1 else 0
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
        if (
          !isMissing(i) && !other.isMissing(i) && self.value != other.values(i)
        ) 1
        else 0
      i += 1
    }
    BufferInt(r)
  }

  def elementwise_abs: BufferInt = {
    BufferIntConstant(
      if (value == Int.MinValue) BufferInt.MissingValue else math.abs(value),
      self.length
    )
  }
  def elementwise_toDouble: BufferDouble = {
    val v =
      if (value == Int.MinValue) BufferDouble.MissingValue else value.toDouble
    val r = Array.fill[Double](self.length)(v)

    BufferDouble(r)
  }
  def elementwise_toLong: BufferLong = {
    val v = if (value == Int.MinValue) BufferLong.MissingValue else value.toLong

    val r = Array.fill[Long](self.length)(v)

    BufferLong(r)
  }

  def elementwise_not: BufferInt = {
    val n = self.length

    val r = if (self.value > 0) 0 else 1
    BufferInt.constant(r, n)
  }
  def elementwise_containedIn(s: Set[Int]): BufferInt = {
    val n = self.length
    val t = if (value != Int.MinValue && s.contains(value)) 1 else 0

    BufferInt.constant(t, n)
  }

  def allInGroups(partitionMap: ra3.BufferInt, numGroups: Int): ra3.BufferInt =
    BufferIntConstant(if (value > 0) 1 else 0, numGroups)

  def anyInGroups(partitionMap: ra3.BufferInt, numGroups: Int): ra3.BufferInt =
    BufferIntConstant(if (value > 0) 1 else 0, numGroups)

  def noneInGroups(partitionMap: ra3.BufferInt, numGroups: Int): ra3.BufferInt =
    BufferIntConstant(if (isMissing(0) || value > 0) 0 else 1, numGroups)

  def hasMissingInGroup(
      partitionMap: ra3.BufferInt,
      numGroups: Int
  ): ra3.BufferInt =
    BufferIntConstant(if (isMissing(0)) 1 else 0, numGroups)

  def maxInGroups(partitionMap: ra3.BufferInt, numGroups: Int): ra3.BufferInt =
    BufferIntConstant(value, numGroups)

  def meanInGroups(
      partitionMap: ra3.BufferInt,
      numGroups: Int
  ): ra3.BufferDouble =
    BufferDouble.constant(
      if (value == Int.MinValue) BufferDouble.MissingValue else value.toDouble,
      numGroups
    )

  def minInGroups(partitionMap: ra3.BufferInt, numGroups: Int): ra3.BufferInt =
    BufferIntConstant(value, numGroups)

  def countDistinctInGroups(
      partitionMap: ra3.BufferInt,
      numGroups: Int
  ): ra3.BufferInt =
    BufferIntConstant(1, numGroups)

  def countInGroups(
      partitionMap: ra3.BufferInt,
      numGroups: Int
  ): ra3.BufferInt = {
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

  def elementwise_eq(other: Int): ra3.BufferInt =
    BufferIntConstant(if (value == other) 1 else 0, length)

  def elementwise_gt(other: Int): ra3.BufferInt =
    BufferIntConstant(if (value > other) 1 else 0, length)

  def elementwise_gteq(other: Int): ra3.BufferInt =
    BufferIntConstant(if (value >= other) 1 else 0, length)

  def elementwise_isMissing: ra3.BufferInt =
    BufferIntConstant(if (isMissing(0)) 1 else 0, length)

  def elementwise_lt(other: Int): ra3.BufferInt =
    BufferIntConstant(if (value < other) 1 else 0, length)

  def elementwise_lteq(other: Int): ra3.BufferInt =
    BufferIntConstant(if (value <= other) 1 else 0, length)

  def elementwise_neq(other: Int): ra3.BufferInt =
    BufferIntConstant(if (value != other) 1 else 0, length)

  def elementwise_printf(s: String): ra3.BufferString = {
    BufferString.constant(s.format(value), length)
  }

}
