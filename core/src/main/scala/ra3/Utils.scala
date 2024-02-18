package ra3
import java.nio.ByteBuffer
import java.nio.channels.WritableByteChannel
import java.nio.channels.ReadableByteChannel
import cats.effect.IO
import scodec.bits.ByteVector
import java.nio.CharBuffer


private[ra3] case class CharArraySubSeq(buff: Array[Char], start: Int, to: Int)
    extends CharSequence {

  override def toString = {
    String.copyValueOf(buff,start,to-start)
  }

  override def equals(other: Any): Boolean = other match {
    case other: CharSequence if this.length == other.length => 
      var b = true 
      var i =  0
      val n = length 
      while (i < n && !b) {
        b = (charAt(i) != other.charAt(i)) 
        i+=1
      }
      b
      case _ => 
        false
  }

  override def hashCode(): Int =  CharBuffer.wrap(buff,start,to-start).hashCode()

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
    columns: Vector[Column]
) {
  def concatenate(other: TableHelper) = {
    assert(columns.size == other.columns.size)
    assert(columns.map(_.tag) == other.columns.map(_.tag))
    TableHelper(
      columns.zip(other.columns).map { case (a, b) => a castAndConcatenate b }
    )
  }
}

private[ra3] object Utils {
  def guessMemoryUsageInMB(s: Int) =
    math.max(5, s.toLong * 64 / 1024 / 1024).toInt
  def guessMemoryUsageInMB(s: Segment) =
    math.max(5, s.numElems.toLong * 64 / 1024 / 1024).toInt
  def guessMemoryUsageInMB(s: Column) =
    {
     val r = math.max(5, s.numElems.toLong * 64 / 1024 / 1024)
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

  def bufferMultiple[
      S <: Segment { type SegmentType = S }
  ](s: Seq[S])(implicit tsc: tasks.TaskSystemComponents): IO[S#BufferType] = {
    val tag = s.head.tag
    IO
      .parSequenceN(32)(s.map(_.buffer))
      .map(b => tag.cat(b.map(_.as(tag).asBufferType): _*))
  }

  def compress(bb: ByteBuffer) = {

    val t1 = System.nanoTime()
    val compressor = new _root_.io.airlift.compress.zstd.ZstdCompressor
    val ar = bb.array()
    val offs = bb.arrayOffset()
    val len = bb.remaining()
    val maxL = compressor.maxCompressedLength(len)
    val compressed = Array.ofDim[Byte](maxL)
    val actualLength =
      compressor.compress(ar, offs, len, compressed, 0, maxL)
    val t2 = System.nanoTime()
    scribe.debug(
      f"zstd compression ratio: ${actualLength.toDouble / len} in ${(t2 - t1) * 1e-6}ms (${actualLength.toDouble/1024/1024}%.2f megabytes)"
    )
    ByteBuffer.wrap(compressed, 0, actualLength)

  }

  def decompress(byteVector: ByteVector) = {
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

  import com.github.plokhotnyuk.jsoniter_scala.core._

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



}
