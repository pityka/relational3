package ra3.bufferimpl
import ra3.join.MutableBuffer
import scala.reflect.ClassTag
object ArrayUtil {
  def filterD(
      arr: Array[Double]
  )(pred: Double => Boolean): Array[Double] = {
    var i = 0
    val buf = MutableBuffer.emptyD
    while (i < arr.length) {
      val v = arr(i)
      if (!v.isNaN && pred(v)) buf.+=(v)
      i += 1
    }
    buf.toArray
  }
  def filterI(
      arr: Array[Int]
  )(pred: Int => Boolean): Array[Int] = {
    var i = 0
    val buf = MutableBuffer.emptyI
    while (i < arr.length) {
      val v = arr(i)
      if (v != ra3.BufferInt.MissingValue && pred(v)) buf.+=(v)
      i += 1
    }
    buf.toArray
  }
  def filterL(
      arr: Array[Long]
  )(pred: Long => Boolean): Array[Long] = {
    var i = 0
    val buf = MutableBuffer.emptyL
    while (i < arr.length) {
      val v = arr(i)
      if (v != ra3.BufferLong.MissingValue && pred(v)) buf.+=(v)
      i += 1
    }
    buf.toArray
  }

  def dropNAD(array: Array[Double]) = filterD(array)(_ => true)
  def dropNAI(array: Array[Int]) = filterI(array)(_ => true)
  def dropNAL(array: Array[Long]) = filterL(array)(_ => true)
  def findD(ar: Array[Double], p: Double => Boolean) = {
    var i = 0
    val buf = MutableBuffer.emptyI
    while (i < ar.length) {
      val v = ar(i)
      if (!v.isNaN && p(v)) buf.+=(i)
      i += 1
    }
    buf.toArray
  }
  def findG[T](ar: Array[T], p: T => Boolean) = {
    var i = 0
    val buf = MutableBuffer.emptyI
    while (i < ar.length) {
      val v = ar(i)
      if (v != null && p(v)) buf.+=(i)
      i += 1
    }
    buf.toArray
  }
  def findL(ar: Array[Long], p: Long => Boolean) = {
    var i = 0
    val buf = MutableBuffer.emptyI
    while (i < ar.length) {
      val v = ar(i)
      if (v != ra3.BufferLong.MissingValue && p(v)) buf.+=(i)
      i += 1
    }
    buf.toArray
  }
  def findI(ar: Array[Int], p: Int => Boolean) = {
    var i = 0
    val buf = MutableBuffer.emptyI
    while (i < ar.length) {
      val v = ar(i)
      if (v != ra3.BufferInt.MissingValue && p(v)) buf.+=(i)
      i += 1
    }
    buf.toArray
  }

  def takeD(
      arr: Array[Double],
      offsets: Array[Int]
  ): Array[Double] = {
    val res = Array.ofDim[Double](offsets.length)
    var i = 0
    while (i < offsets.length) {
      val idx = offsets(i)
      if (idx == -1)
        res(i) = Double.NaN
      else
        res(i) = arr(idx)
      i += 1
    }
    res
  }
  def takeL(
      arr: Array[Long],
      offsets: Array[Int]
  ): Array[Long] = {
    val res = Array.ofDim[Long](offsets.length)
    var i = 0
    while (i < offsets.length) {
      val idx = offsets(i)
      if (idx == -1)
        res(i) = ra3.BufferLong.MissingValue
      else
        res(i) = arr(idx)
      i += 1
    }
    res
  }
  def takeI(
      arr: Array[Int],
      offsets: Array[Int]
  ): Array[Int] = {
    val res = Array.ofDim[Int](offsets.length)
    var i = 0
    while (i < offsets.length) {
      val idx = offsets(i)
      if (idx == -1)
        res(i) = ra3.BufferInt.MissingValue
      else
        res(i) = arr(idx)
      i += 1
    }
    res
  }

  def range(from: Int, until: Int): Array[Int] = {
    if (from >= until) Array.ofDim[Int](0)
    else {

      val sz = until - from
      var i = from
      var k = 0
      val arr = Array.ofDim[Int](sz)
      while (k < sz) {
        arr(k) = i
        k += 1
        i += 1
      }
      arr
    }
  }

  def flattenI(
      arrs: Seq[Array[Int]]
  ): Array[Int] = {
    val size = arrs.map(_.length).sum
    val newArr = new Array[Int](size)
    var i = 0

    arrs.foreach { a =>
      val l = a.length
      System.arraycopy(a, 0, newArr, i, l)
      i += l
    }

    newArr
  }
  def flattenL(
      arrs: Seq[Array[Long]]
  ): Array[Long] = {
    val size = arrs.map(_.length).sum
    val newArr = new Array[Long](size)
    var i = 0

    arrs.foreach { a =>
      val l = a.length
      System.arraycopy(a, 0, newArr, i, l)
      i += l
    }

    newArr
  }
  def flattenD(
      arrs: Seq[Array[Double]]
  ): Array[Double] = {
    val size = arrs.map(_.length).sum
    val newArr = new Array[Double](size)
    var i = 0

    arrs.foreach { a =>
      val l = a.length
      System.arraycopy(a, 0, newArr, i, l)
      i += l
    }

    newArr
  }
  def flattenG[T: ClassTag](
      arrs: Seq[Array[T]]
  ): Array[T] = {
    val size = arrs.map(_.length).sum
    val newArr = new Array[T](size)
    var i = 0

    arrs.foreach { a =>
      val l = a.length
      System.arraycopy(a, 0, newArr, i, l)
      i += l
    }

    newArr
  }
}
