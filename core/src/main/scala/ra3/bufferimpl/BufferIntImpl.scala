package ra3.bufferimpl
import ra3._
import cats.effect.IO
import java.nio.ByteBuffer
import java.nio.ByteOrder
import tasks.{TaskSystemComponents, SharedFile}
private[ra3] trait BufferIntImpl { self: BufferIntInArray =>
  
  def positiveLocations: BufferInt = {
    import org.saddle._
    BufferInt(
      values.toVec.find(_ > 0).toArray
    )
  }

  

  override def toString =
    s"BufferInt(n=${values.length}: ${values.take(5).mkString(", ")} ..})"

  /* Returns a buffer of numGroups. It may overflow. */
  def sumGroups(partitionMap: BufferInt, numGroups: Int): BufferType = {
    assert(partitionMap.length == length)
    val ar = Array.ofDim[Int](numGroups)
    var i = 0
    val n = partitionMap.length
    while (i < n) {
      if (!isMissing(i)) { ar(partitionMap.raw(i)) += values(i) }
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
    val idx =
      if (lessThan)
        values.toVec.find(_ <= c)
      else values.toVec.find(_ >= c)

    BufferInt(idx.toArray)
  }

  def toSeq = values.toSeq

  def cdf(numPoints: Int): (BufferInt, BufferDouble) = {
    val percentiles =
      ((0 until (numPoints - 1)).map(i => i * (1d / (numPoints - 1))) ++ List(
        1d
      )).distinct
    val sorted = org.saddle.array.sort[Int](values)
    val cdf = percentiles.map { p =>
      val idx = (p * (values.length - 1)).toInt
      (sorted(idx), p)
    }

    val x = BufferInt(cdf.map(_._1).toArray)
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
    BufferInt(values.toVec.find(_ == i).toArray)
  }

  override def take(locs: Location): BufferInt = locs match {
    case Slice(start, until) =>
      val r = Array.ofDim[Int](until - start)
      System.arraycopy(values, start, r, 0, until - start)
      BufferInt(r)
    case idx: BufferInt =>
      BufferInt(values.toVec.take(idx.values).toArray)
  }

  override def isMissing(l: Int): Boolean = {
    values(l) == Int.MinValue
  }
  override def hashOf(l: Int): Long = {
    scala.util.hashing.byteswap32(values(l)).toLong
  }

  override def toSegment(
      name: LogicalPath
  )(implicit tsc: TaskSystemComponents): IO[SegmentInt] = {
    if (values.length == 0) IO.pure(SegmentInt(None, 0, None))
    else
      IO {
        val bb =
          ByteBuffer.allocate(4 * values.length).order(ByteOrder.LITTLE_ENDIAN)
        bb.asIntBuffer().put(values)
        fs2.Stream.chunk(fs2.Chunk.byteBuffer(bb))
      }.flatMap { stream =>
        val minmax = if (values.length == 0) None else {
          Some(
            (values.min,
            values.max)
          )
        } 
        SharedFile
          .apply(stream, name.toString)
          .map(sf => SegmentInt(Some(sf), values.length, minmax))
      }

  }

    def elementwise_*(other: BufferType) : BufferType = ???
  def elementwise_+(other: BufferType) : BufferType = ???
  def elementwise_eq(other: BufferType) : BufferInt = ???
  def elementwise_gt(other: BufferType) : BufferInt = ???
  def elementwise_gteq(other: BufferType) : BufferInt = ???
  def elementwise_lt(other: BufferType) : BufferInt = ???
  def elementwise_lteq(other: BufferType) : BufferInt = ???
  def elementwise_neq(other: BufferType) : BufferInt = ???
  
  def elementwise_toDouble : BufferDouble = ???
  def elementwise_toLong : BufferDouble = ???

}
