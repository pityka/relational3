package ra3.bufferimpl
import ra3._
import cats.effect.IO
import java.nio.ByteBuffer
import java.nio.ByteOrder
import tasks.{TaskSystemComponents, SharedFile}

private[ra3] object CharSequenceOrdering
    extends scala.math.Ordering[CharSequence] { self =>

  def compare(x: CharSequence, y: CharSequence): Int =
    if (x == BufferString.missing && y == BufferString.missing) 0
    else if (x == BufferString.missing) -1
    else if (y == BufferString.missing) 1
    else if (x eq y) 0
    else CharSequence.compare(x, y)

}

private[ra3] trait BufferStringImpl { self: BufferString =>
  def positiveLocations: BufferInt = {
    import org.saddle._
    BufferInt(
      values.toVec.find(s => s != BufferString.missing && s.length > 0).toArray
    )
  }

  override def toString =
    s"BufferString(n=${values.length}: ${values.take(5).mkString(", ")} ..})"

  def sumGroups(partitionMap: BufferInt, numGroups: Int): BufferType = ???

  /** Find locations at which _ <= other[0] or _ >= other[0] holds returns
    * indexes
    */
  override def findInequalityVsHead(
      other: BufferType,
      lessThan: Boolean
  ): BufferInt = {
    import org.saddle._
    val ord = CharSequenceOrdering
    val c = other.values(0)
    val idx =
      if (lessThan)
        values.toVec.find(x => x != BufferString.missing && ord.lteq(x, c))
      else values.toVec.find(x => x != BufferString.missing && ord.gteq(x, c))

    BufferInt(idx.toArray)
  }

  def toSeq = values.toSeq

  def cdf(numPoints: Int): (BufferString, BufferDouble) = {
    val percentiles =
      ((0 until (numPoints - 1)).map(i => i * (1d / (numPoints - 1))) ++ List(
        1d
      )).distinct
    val sorted =
      values.sorted(CharSequenceOrdering) // values.sorted(CharSequenceOrderIng)
    val cdf = percentiles.map { p =>
      val idx = (p * (values.length - 1)).toInt
      (sorted(idx), p)
    }

    val x = BufferString(cdf.map(_._1).toArray)
    val y = BufferDouble(cdf.map(_._2).toArray)
    (x, y)
  }

  def length = values.length


    

  def groups = {
    import org.saddle.{Buffer => _, _}
    val idx = Index(values.map(_.toString))
    val uniques = idx.uniques
    val counts = idx.counts
    val map = Array.ofDim[Int](values.length)
    var i = 0
    while (i < values.length) {
      map(i) = uniques.getFirst(values(i).toString)
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
    import org.saddle.{Buffer => _, _}
    val idx1 = Index(values.map(_.toString))
    val idx2 = Index(other.values.map(_.toString))
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

  override def take(locs: Location): BufferString = locs match {
    case Slice(start, until) =>
      val r = Array.ofDim[CharSequence](until - start)
      System.arraycopy(values, start, r, 0, until - start)
      BufferString(r)
    case idx: BufferInt =>
      import org.saddle.{Buffer => _, _}
      BufferString(values.toVec.take(idx.values).fillNA(_ => BufferString.missing).toArray)
  }

  override def isMissing(l: Int): Boolean = {
    values(l) == s"${Char.MinValue}"
  }
  private val hasher = scala.util.hashing.MurmurHash3.arrayHashing[Int]
  override def hashOf(l: Int): Long = {
    hasher.hash(values(l).codePoints.toArray).toLong
  }

  override def toSegment(
      name: LogicalPath
  )(implicit tsc: TaskSystemComponents): IO[SegmentString] = {
    if (values.length == 0) IO.pure(SegmentString(None, 0, None))
    else
      IO {
        val byteSize = values.map(v => (v.length * 2L) + 4).sum
        if (byteSize > Int.MaxValue - 100)
          fs2.Stream.raiseError[IO](
            new RuntimeException(
              "String buffers longer than what fits into an array not implemented"
            )
          )
        else {
          val bb =
            ByteBuffer.allocate(byteSize.toInt).order(ByteOrder.BIG_ENDIAN)
          values.foreach { str =>
            bb.putInt(str.length)
            var i = 0
            while (i < str.length) {
              bb.putChar(str.charAt(i))
              i += 1
            }
          }
          bb.flip()
          fs2.Stream.chunk(fs2.Chunk.byteBuffer(bb))
        }
      }.flatMap { stream =>
        val minmax: Option[(String, String)] = {
          if (values.length > 0)
            Some(
              (
                values.min(CharSequenceOrdering).toString,
                values.max(CharSequenceOrdering).toString
              )
            )
          else None
        }

        SharedFile
          .apply(stream, name.toString)
          .map(sf => SegmentString(Some(sf), values.length, minmax))
      }

  }

  def elementwise_+(other: BufferType) : BufferType = {
    assert(other.length == self.length)
    var i = 0
    val n = self.length
    val r = Array.ofDim[CharSequence](n)
    while (i < n) {
      r(i) = self.values(i).toString + other.values(i).toString()
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
      r(i) = if (self.values(i) == other.values(i)) 1 else 0
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
      r(i) = if (CharSequenceOrdering.gt(self.values(i) , other.values(i))) 1 else 0
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
      r(i) = if (CharSequenceOrdering.gteq(self.values(i) , other.values(i))) 1 else 0
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
      r(i) = if (CharSequenceOrdering.lt(self.values(i) , other.values(i))) 1 else 0
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
      r(i) = if (CharSequenceOrdering.lteq(self.values(i) , other.values(i))) 1 else 0
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
      r(i) = if (self.values(i) != other.values(i)) 1 else 0
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_length: BufferInt  = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)
    while (i < n) {
      r(i) = self.values(i).length() 
      i += 1
    }
    BufferInt(r)
  }
  def elementwise_nonempty: BufferInt  = {
    var i = 0
    val n = self.length
    val r = Array.ofDim[Int](n)
    while (i < n) {
      r(i) = if (self.values(i).length() > 0) 1 else 0
      i += 1
    }
    BufferInt(r)
  }

}
