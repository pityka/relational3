package ra3
import java.nio.ByteBuffer
import java.nio.channels.WritableByteChannel
import java.nio.channels.ReadableByteChannel
import cats.effect.IO
import scodec.bits.ByteVector
import java.nio.CharBuffer
import java.io.InputStream
import tasks.TaskSystemComponents

object Fnv1 {
  inline def hashByteBuffer(buff: ByteBuffer) = {
    // // fnv 32bit
    var hash = 0x811c9dc5
    val n = buff.remaining
    var i = 0
    while (i < n) {
      hash *= 16777619
      hash ^= (buff.get(i) & 0xff)
      i += 1
    }
    hash.toInt
  }
}
object Murmur3 {
  /* Derived from https://github.com/scala/scala/blob/v2.13.14/src/library/scala/util/hashing/MurmurHash3.scala#L330
   *
   * Scala (https://www.scala-lang.org)
   *
   * Copyright EPFL and Lightbend, Inc.
   *
   * Licensed under Apache License 2.0
   * (http://www.apache.org/licenses/LICENSE-2.0).
   *
   * See the NOTICE file distributed with this work for
   * additional information regarding copyright ownership.
   */
  inline def hashByteBuffer(buff: ByteBuffer) = {
    var len = buff.remaining()
    var h = 0x3c074a61

    // Body
    var i = 0
    while (len >= 4) {
      var k = buff.get(i + 0) & 0xff
      k |= (buff.get(i + 1) & 0xff) << 8
      k |= (buff.get(i + 2) & 0xff) << 16
      k |= (buff.get(i + 3) & 0xff) << 24

      h = Murmur3.mix(h, k)

      i += 4
      len -= 4
    }

    // Tail
    var k = 0
    if (len == 3) k ^= (buff.get(i + 2) & 0xff) << 16
    if (len >= 2) k ^= (buff.get(i + 1) & 0xff) << 8
    if (len >= 1) {
      k ^= (buff.get(i + 0) & 0xff)
      h = Murmur3.mixLast(h, k)
    }

    // Finalization
    Murmur3.finalizeHash(h, buff.remaining())
  }
  inline def hashLong(l: Long) = {
    var h = 0x3c074a61

    // Body
    var i0 = (l & 0xff).toInt
    var i1 = (l >> 32).toInt

    h = Murmur3.mix(h, i0)
    h = Murmur3.mixLast(h, i1)

    // Finalization
    Murmur3.finalizeHash(h, 2)
  }
  inline def hashDouble(l: Double) =
    hashLong(java.lang.Double.doubleToLongBits(l))
  inline def hashInt(l: Int) = {
    var h = 0x3c074a61

    // Body

    h = Murmur3.mixLast(h, l)

    // Finalization
    Murmur3.finalizeHash(h, 2)
  }

  import java.lang.Integer.{rotateLeft => rotl}

  /** Mix in a block of data into an intermediate hash value. */
  inline def mix(hash: Int, data: Int): Int = {
    var h = mixLast(hash, data)
    h = rotl(h, 13)
    h * 5 + 0xe6546b64
  }

  /** May optionally be used as the last mixing step. Is a little bit faster
    * than mix, as it does no further mixing of the resulting hash. For the last
    * element this is not necessary as the hash is thoroughly mixed during
    * finalization anyway.
    */
  inline def mixLast(hash: Int, data: Int): Int = {
    var k = data

    k *= 0xcc9e2d51
    k = rotl(k, 15)
    k *= 0x1b873593

    hash ^ k
  }

  /** Finalize a hash to incorporate the length and make sure all bits
    * avalanche.
    */
  inline def finalizeHash(hash: Int, length: Int): Int = avalanche(
    hash ^ length
  )

  /** Force all bits of the hash to avalanche. Used for finalizing the hash. */
  private final def avalanche(hash: Int): Int = {
    var h = hash

    h ^= h >>> 16
    h *= 0x85ebca6b
    h ^= h >>> 13
    h *= 0xc2b2ae35
    h ^= h >>> 16

    h
  }
}

private[ra3] case class ByteBufferAsCharSequence(buff: ByteBuffer)
    extends CharSequence {

  override def toString = {
    buff.asCharBuffer().toString
  }

  def toCharArray = {
    val a = Array.ofDim[Char](length)
    0 until length foreach { i => a(i) = charAt(i) }
    a
  }

  override def equals(other: Any): Boolean =
    other.asInstanceOf[Matchable] match {
      case other: CharSequence if this.length == other.length =>
        var b = true
        var i = 0
        val n = length
        while (i < n && b) {
          b = (charAt(i) == other.charAt(i))
          i += 1
        }

        b
      case _ =>
        false
    }

  override def hashCode(): Int = {

    Murmur3.hashByteBuffer(buff)

  }

  override def length(): Int = buff.remaining() / 2

  override def charAt(index: Int): Char = {
    buff.getChar(index * 2)
  }

  override def subSequence(start: Int, end: Int): CharSequence =
    throw new NotImplementedError

}
private[ra3] case class CharArraySubSeq(buff: Array[Char], start: Int, to: Int)
    extends CharSequence {

  override def toString = {
    String.copyValueOf(buff, start, to - start)
  }

  override def equals(other: Any): Boolean =
    other.asInstanceOf[Matchable] match {
      case other: CharSequence if this.length == other.length =>
        var b = true
        var i = 0
        val n = length
        while (i < n && b) {
          b = (charAt(i) == other.charAt(i))
          i += 1
        }
        b
      case _ =>
        false
    }

  override def hashCode(): Int =
    CharBuffer.wrap(buff, start, to - start).hashCode()

  override def length(): Int = to - start

  override def charAt(index: Int): Char = {
    val rel = index + start
    if (rel >= to) throw new IndexOutOfBoundsException
    else buff(rel)
  }

  override def subSequence(start: Int, end: Int): CharSequence =
    throw new NotImplementedError

}

private[ra3] case class TableHelper(
    columns: Vector[TaggedColumn]
) {
  def concatenate(other: TableHelper) = {
    assert(columns.size == other.columns.size)
    assert(columns.map(_.tag) == other.columns.map(_.tag))
    TableHelper(
      columns.zip(other.columns).map { case (a, b) => a `castAndConcatenate` b }
    )
  }
}

private[ra3] object Utils {

  def guessMemoryUsageInMB(s: Int) =
    math.max(5, s.toLong * 64 / 1024 / 1024).toInt
  def guessMemoryUsageInMB(s: Segment) =
    math.max(5, s.numBytes * 2 / 1024 / 1024).toInt
  def guessMemoryUsageInMB(tc: TaggedColumn): Int =
    guessMemoryUsageInMB(tc.tag)(tc.column)
  def guessMemoryUsageInMB(tag: ColumnTag)(s: tag.ColumnType): Int = {
    val r = math.max(5, tag.numBytes(s) * 2 / 1024 / 1024)
    r.toInt
  }
  def writeFully(bb: ByteBuffer, channel: WritableByteChannel) = {
    bb.rewind
    while (bb.hasRemaining) {
      channel.write(bb)
    }
  }
  def readFully(
      bb: ByteBuffer,
      channel: ReadableByteChannel
  ): Unit = {
    bb.clear
    var i = 0
    while (bb.hasRemaining && i >= 0) {
      i = channel.read(bb)
    }
    bb.flip
  }

  def bufferMultiple(tag: ColumnTag)(
      s: Seq[tag.SegmentType]
  )(implicit tsc: tasks.TaskSystemComponents): IO[tag.BufferType] = {

    IO.cede >> IO
      .parSequenceN(32)(s.map(tag.buffer))
      .map(b => tag.cat(b*))
      .logElapsed.guarantee(IO.cede)
  }

  val skipCompressAll = false

  def compress(bb: ByteBuffer, skipCompress: Boolean = false) = {
    if (skipCompress || skipCompressAll) bb
    else {
      // bb
      val compressor = new _root_.io.airlift.compress.zstd.ZstdCompressor
      val ar = bb.array()
      val offs = bb.arrayOffset()
      val len = bb.remaining()
      val maxL = compressor.maxCompressedLength(len)
      val compressed = Array.ofDim[Byte](maxL)
      val actualLength =
        compressor.compress(ar, offs, len, compressed, 0, maxL)      
      ByteBuffer.wrap(compressed, 0, actualLength)
    }

  }

  def decompress(byteVector: ByteVector, skipCompress: Boolean = false) = {
    if (skipCompress || skipCompressAll) java.nio.ByteBuffer.wrap(byteVector.toArrayUnsafe)
    else {
      val compressed = byteVector.toArrayUnsafe
      val decompressor =
        new _root_.io.airlift.compress.zstd.ZstdDecompressor
      val ar = compressed
      val offs = 0
      val len = compressed.length
      val decLen = _root_.io.airlift.compress.zstd.ZstdDecompressor
        .getDecompressedSize(ar, offs, len)
        .toInt
      val decompressedBuffer = Array.ofDim[Byte](decLen)
      decompressor.decompress(
        ar,
        offs,
        len,
        decompressedBuffer,
        0,
        decLen
      )
      java.nio.ByteBuffer.wrap(decompressedBuffer)
    }

  }

  import com.github.plokhotnyuk.jsoniter_scala.core.*

  val customDoubleCodec = new JsonValueCodec[Double] {
    val nullValue: Double = 0.0f

    def decodeValue(in: JsonReader, default: Double): Double =
      if (in.isNextToken('"')) {
        in.rollbackToken()
        val len = in.readStringAsCharBuf()
        if (in.isCharBufEqualsTo(len, "NaN")) Double.NaN
        else if (in.isCharBufEqualsTo(len, "Infinity"))
          Double.PositiveInfinity
        else if (in.isCharBufEqualsTo(len, "-Infinity"))
          Double.NegativeInfinity
        else in.decodeError("illegal double")
      } else {
        in.rollbackToken()
        in.readDouble()
      }

    def encodeValue(x: Double, out: JsonWriter): _root_.scala.Unit =
      if (_root_.java.lang.Double.isFinite(x)) out.writeVal(x)
      else
        out.writeNonEscapedAsciiVal {
          if (x != x) "NaN"
          else if (x >= 0) "Infinity"
          else "-Infinity"
        }
  }

  val charSequenceCodec: JsonValueCodec[CharSequence] =
    new JsonValueCodec[CharSequence] {
      override def decodeValue(in: JsonReader, default: CharSequence): String =
        if (in.isNextToken('"')) {
          in.rollbackToken()
          in.readString(null)
        } else {
          in.decodeError("expected string")
        }

      override def encodeValue(
          x: CharSequence,
          out: JsonWriter
      ): Unit = out.writeVal(x.toString)

      override val nullValue: CharSequence = null
    }

  def gzip(is: InputStream): InputStream =
    new GzipCompressorInputStream(is, true)

  private[ra3] def bufferBeforeI32[C](
      arg: DI32
  )(fun: BufferInt => C)(implicit tsc: TaskSystemComponents) = {
    val tag = ColumnTag.I32
    bbImpl(tag)(
      arg.map(tag.makeTaggedSegments).left.map(tag.makeTaggedBuffer)
    )(fun)
  }
  private[ra3] def bufferBeforeF64[C](
      arg: DF64
  )(fun: BufferDouble => C)(implicit tsc: TaskSystemComponents) = {
    val tag = ColumnTag.F64
    bbImpl(tag)(
      arg.map(tag.makeTaggedSegments).left.map(tag.makeTaggedBuffer)
    )(fun)
  }
  private[ra3] def bufferBeforeI64[C](
      arg: DI64
  )(fun: BufferLong => C)(implicit tsc: TaskSystemComponents) = {
    val tag = ColumnTag.I64
    bbImpl(tag)(
      arg.map(tag.makeTaggedSegments).left.map(tag.makeTaggedBuffer)
    )(fun)
  }
  private[ra3] def bufferBeforeString[C](
      arg: DStr
  )(fun: BufferString => C)(implicit tsc: TaskSystemComponents) = {
    val tag = ColumnTag.StringTag
    bbImpl(tag)(
      arg.map(tag.makeTaggedSegments).left.map(tag.makeTaggedBuffer)
    )(fun)
  }
  private[ra3] def bufferBeforeInstant[C](
      arg: DInst
  )(fun: BufferInstant => C)(implicit tsc: TaskSystemComponents) = {
    val tag = ColumnTag.Instant
    bbImpl(tag)(
      arg.map(tag.makeTaggedSegments).left.map(tag.makeTaggedBuffer)
    )(fun)
  }
  private[ra3] def bbImpl[C](tag: ColumnTag)(
      arg: Either[tag.TaggedBufferType, tag.TaggedSegmentsType]
  )(fun: tag.BufferType => C)(implicit tsc: TaskSystemComponents) =
    arg.fold(
      value => IO.pure(Left(fun(tag.buffer(value)))),
      value =>
        ra3.Utils
          .bufferMultiple(tag)(tag.segments(value))
          .map(b => Left(fun(b)))
    )

}
