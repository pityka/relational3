package ra3.bufferimpl
import ra3._
import cats.effect.IO
import java.nio.ByteBuffer
import java.nio.ByteOrder
import tasks.{TaskSystemComponents, SharedFile}

private[ra3] trait BufferInstantImpl { self: BufferInstant =>
  def positiveLocations: BufferInt = {
    import org.saddle._
    BufferInt(
      values.toVec.find(_ > 0L).toArray
    )
  }

  override def toString =
    s"BufferInstant(n=${values.length}: ${values.take(5).mkString(", ")} ..})"

  def sumGroups(partitionMap: BufferInt, numGroups: Int): BufferType = ???

  /** Find locations at which _ <= other[0] or _ >= other[0] holds returns
    * indexes
    */
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

  def toSeq = values.toSeq

  def cdf(numPoints: Int): (BufferInstant, BufferDouble) = {
    val percentiles =
      ((0 until (numPoints - 1)).map(i => i * (1d / (numPoints - 1))) ++ List(
        1d
      )).distinct
    val sorted = org.saddle.array.sort[Long](values)
    val cdf = percentiles.map { p =>
      val idx = (p * (values.length - 1)).toInt
      (sorted(idx), p)
    }

    val x = BufferInstant(cdf.map(_._1).toArray)
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
    BufferInstant(r)

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


  override def take(locs: Location): BufferInstant = locs match {
    case Slice(start, until) =>
      val r = Array.ofDim[Long](until - start)
      System.arraycopy(values, start, r, 0, until - start)
      BufferInstant(r)
    case idx: BufferInt =>
      BufferInstant(values.toVec.take(idx.values).toArray)
  }

  override def isMissing(l: Int): Boolean = {
    values(l) == Long.MinValue
  }
  override def hashOf(l: Int): Long = {
    values(l)
  }

  override def toSegment(
      name: LogicalPath
  )(implicit tsc: TaskSystemComponents): IO[SegmentInstant] = {
    if (values.length == 0) IO.pure(SegmentInstant(None, 0, None))
    else
      IO {
        val bb =
          ByteBuffer.allocate(8 * values.length).order(ByteOrder.LITTLE_ENDIAN)
        bb.asLongBuffer().put(values)
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
          .map(sf => SegmentInstant(Some(sf), values.length, minmax))
      }

  }

  def elementwise_+(other: BufferLong) : BufferType = ???
  def elementwise_eq(other: BufferType) : BufferInt = ???
  def elementwise_gt(other: BufferType) : BufferInt = ???
  def elementwise_gteq(other: BufferType) : BufferInt = ???
  def elementwise_lt(other: BufferType) : BufferInt = ???
  def elementwise_lteq(other: BufferType) : BufferInt = ???
  def elementwise_neq(other: BufferType) : BufferInt = ???

  def elementwise_toLongMillis : BufferDouble = ???

}
