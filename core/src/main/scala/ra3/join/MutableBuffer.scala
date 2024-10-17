/** This code is copied from https://github.com/denisrosset/meta The MIT License
  * (MIT) \=====================
  *
  * Copyright (c) 2015 Denis Rosset Hash set and hash map implementations based
  * on code (c) 2012-2014 Eirk Osheim
  *
  * Permission is hereby granted, free of charge, to any person obtaining a copy
  * of this software and associated documentation files (the "Software"), to
  * deal in the Software without restriction, including without limitation the
  * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
  * sell copies of the Software, and to permit persons to whom the Software is
  * furnished to do so, subject to the following conditions:
  *
  * The above copyright notice and this permission notice shall be included in
  * all copies or substantial portions of the Software.
  *
  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
  * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
  * IN THE SOFTWARE.
  *
  * Modifications:
  *   - specialize
  */
package ra3.join

import scala.reflect.ClassTag
import scala.collection.mutable.{Buffer => SBuffer}
import scala.collection.mutable.ListBuffer

final class MBufferDouble(
    private var arrays: SBuffer[Array[Double]],
    var length: Int
) {

  private type V = Double
  private val ctV = implicitly[ClassTag[Double]]

  private var cursor = 0

  final def toArray: Array[V] = {
    val res = ctV.newArray(length.toInt)
    var i = 0
    var j = 0
    arrays.foreach { array =>
      val l =
        if (j == arrays.length - 1) length - i
        else array.length
      Array.copy(array, 0, res, i, l)
      j += 1
      i += array.length

    }
    res
  }

  final def +=(elem: V): this.type = {
    arrays.last(cursor) = elem
    length += 1
    cursor += 1
    fill()
    this
  }

  private inline final def fill() = {
    if (arrays.last.length <= cursor) {
      val newArray =
        ctV.newArray(math.min(MutableBuffer.maxSize, arrays.last.length * 2))
      arrays.append(newArray)
      cursor = 0
    }

  }

}
final class MBufferLong(
    private var arrays: SBuffer[Array[Long]],
    var length: Int
) {

  private type V = Long
  private val ctV = implicitly[ClassTag[Long]]

  private var cursor = 0

  final def toArray: Array[V] = {
    val res = ctV.newArray(length.toInt)
    var i = 0
    var j = 0
    arrays.foreach { array =>
      val l =
        if (j == arrays.length - 1) length - i
        else array.length
      Array.copy(array, 0, res, i, l)
      j += 1
      i += array.length

    }
    res
  }

  final def +=(elem: V): this.type = {
    arrays.last(cursor) = elem
    length += 1
    cursor += 1
    fill()
    this
  }

  private inline final def fill() = {
    if (arrays.last.length <= cursor) {
      val newArray =
        ctV.newArray(math.min(MutableBuffer.maxSize, arrays.last.length * 2))
      arrays.append(newArray)
      cursor = 0
    }

  }

}
final class MBufferInt(
    private var arrays: SBuffer[Array[Int]],
    var length: Int
) {

  private type V = Int
  private val ctV = implicitly[ClassTag[Int]]

  private var cursor = 0

  final def toArray: Array[V] = {
    val res = ctV.newArray(length.toInt)
    var i = 0
    var j = 0
    arrays.foreach { array =>
      val l =
        if (j == arrays.length - 1) length - i
        else array.length
      Array.copy(array, 0, res, i, l)
      j += 1
      i += array.length

    }
    res
  }

  final def +=(elem: V): this.type = {
    arrays.last(cursor) = elem
    length += 1
    cursor += 1
    fill()
    this
  }

  private inline final def fill() = {
    if (arrays.last.length <= cursor) {
      val newArray =
        ctV.newArray(math.min(MutableBuffer.maxSize, arrays.last.length * 2))
      arrays.append(newArray)
      cursor = 0
    }

  }

}
final class MBufferG[T](
    private var arrays: SBuffer[Array[T]],
    var length: Int
)(implicit ctV: ClassTag[T]) {

  private type V = T

  private var cursor = 0

  final def toArray: Array[V] = {
    val res = ctV.newArray(length.toInt)
    var i = 0
    var j = 0
    arrays.foreach { array =>
      val l =
        if (j == arrays.length - 1) length - i
        else array.length
      Array.copy(array, 0, res, i, l)
      j += 1
      i += array.length

    }
    res
  }

  final def +=(elem: V): this.type = {
    arrays.last(cursor) = elem
    length += 1
    cursor += 1
    fill()
    this
  }

  private inline final def fill() = {
    if (arrays.last.length <= cursor) {
      val newArray =
        ctV.newArray(math.min(MutableBuffer.maxSize, arrays.last.length * 2))
      arrays.append(newArray)
      cursor = 0
    }

  }

}

object MutableBuffer {

  val startSize = 256
  val maxSize = 16777216

  def emptyD: MBufferDouble =
    new MBufferDouble(ListBuffer(new Array[Double](startSize)), 0)
  def emptyD(size: Int): MBufferDouble =
    new MBufferDouble(ListBuffer(new Array[Double](size)), 0)
  def emptyI: MBufferInt =
    new MBufferInt(ListBuffer(new Array[Int](startSize)), 0)
  def emptyI(size: Int): MBufferInt =
    new MBufferInt(ListBuffer(new Array[Int](size)), 0)
  def emptyL: MBufferLong =
    new MBufferLong(ListBuffer(new Array[Long](startSize)), 0)
  def emptyL(size: Int): MBufferLong =
    new MBufferLong(ListBuffer(new Array[Long](size)), 0)
  def emptyG[T: ClassTag]: MBufferG[T] =
    new MBufferG(ListBuffer(new Array[T](startSize)), 0)

}
