package ra3

private[ra3] object BitSet {

  final val LogWL = 6
  final val WordLength = 64
  final val MaxSize = (Int.MaxValue >> LogWL) + 1

  def allocate(i: Int) = new BitSetBuilder(
    Array.ofDim[Long](math.ceil(i / 64.0).toInt)
  )

}

private[ra3] final class BitSetBuilder(final val elems: Array[Long]) {
  import BitSet._

  def toBitSet = BitSet(elems.toVector)

  def array = (0 until elems.size * WordLength).map { i =>
    if (contains(i)) 1 else 0
  } toArray

  override def toString = {
    (0 until elems.size * WordLength).map { i =>
      if (contains(i)) "1" else "0"
    }.mkString
  }

  @inline
  def capacity = elems.length * WordLength

  @inline
  def contains(elem: Int): Boolean = {
    val idx = elem >> LogWL
    val n = elems.length

    0 <= elem && n != 0 && n > idx && (elems(idx) & (1L << elem)) != 0L
  }

  def addOne(elem: Int) = {
    val idx = elem >> LogWL
    val r = elem & 63
    elems(idx) = elems(idx) | (1L << r)
  }

}
private[ra3] final case class BitSet(final val elems: Vector[Long]) {
  import BitSet._

  def array = (0 until elems.size * WordLength).map { i =>
    if (contains(i)) 1 else 0
  } toArray

  override def toString = {
    (0 until elems.size * WordLength).map { i =>
      if (contains(i)) "1" else "0"
    }.mkString
  }

  @inline
  def capacity = elems.length * WordLength

  @inline
  def contains(elem: Int): Boolean = {
    val idx = elem >> LogWL
    val n = elems.length

    0 <= elem && n != 0 && n > idx && (elems(idx) & (1L << elem)) != 0L
  }

}
