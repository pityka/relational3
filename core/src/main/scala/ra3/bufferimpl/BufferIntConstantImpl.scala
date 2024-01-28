package ra3.bufferimpl
import cats.effect.IO
import ra3._
import tasks.{TaskSystemComponents}
private[ra3] trait BufferIntConstantImpl { self: BufferIntConstant =>

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
    val ar = Array.ofDim[Int](numGroups)
    var i = 0
    val n = partitionMap.length
    while (i < n) {
      if (!isMissing(i)) { ar(partitionMap.raw(i)) += value }
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
    val c = other.raw(0)

    if (lessThan) {
      if (value <= c) {
        BufferInt(array.range(0, length))
      } else BufferInt.empty
    } else if (value >= c) {
      BufferInt(array.range(0, length))
    } else BufferInt.empty

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
    if (values.length == 0) IO.pure(SegmentInt(None, 0, None))
    else
      IO.pure {

        val minmax =
          if (length == 0) None
          else {
            Some(
              (value, value)
            )
          }
        SegmentInt(None, values.length, minmax)
      }

  }

  // Elementwise operations

  def elementwise_*=(other: BufferType): Unit =  {
    ???
  }
  def elementwise_*(other: BufferDouble): BufferDouble = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Double](n)
    while (i < n) {
      r(i) = self.value.toDouble * other.values(i)
      i += 1
    }
    BufferDouble(r)
  }
  def elementwise_+=(other: BufferType): Unit = {
    ???
  }
  def elementwise_+(other: ra3.BufferDouble): ra3.BufferDouble = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Double](n)
    while (i < n) {
      r(i) = self.value + other.values(i)
      i += 1
    }
    BufferDouble(r)
  }

  def elementwise_&&(other: BufferType): BufferInt = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)
    while (i < n) {
      r(i) = if (self.value > 0 && other.values(i) > 0) 1 else 0
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
      r(i) = if (self.value > 0 || other.values(i) > 0) 1 else 0
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
      r(i) = if (self.value == other.values(i)) 1 else 0
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
      r(i) = if (self.value > other.values(i)) 1 else 0
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
      r(i) = if (self.value >= other.values(i)) 1 else 0
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
      r(i) = if (self.value < other.values(i)) 1 else 0
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
      r(i) = if (self.value <= other.values(i)) 1 else 0
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
      r(i) = if (self.value != other.values(i)) 1 else 0
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
      r(i) = if (self.value == other.values(i)) 1 else 0
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
      r(i) = if (self.value > other.values(i)) 1 else 0
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
      r(i) = if (self.value >= other.values(i)) 1 else 0
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
      r(i) = if (self.value < other.values(i)) 1 else 0
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
      r(i) = if (self.value <= other.values(i)) 1 else 0
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
      r(i) = if (self.value != other.values(i)) 1 else 0
      i += 1
    }
    BufferInt(r)
  }


  def elementwise_toDouble: BufferDouble =  {
    val r = Array.fill[Double](self.length)(self.value.toDouble)
    
    BufferDouble(r)
  }
  def elementwise_toLong: BufferLong = {
    val r = Array.fill[Long](self.length)(self.value.toLong)
    
    BufferLong(r)
  }

}
