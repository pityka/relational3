package ra3
import java.nio.ByteBuffer
import java.nio.channels.WritableByteChannel
import java.nio.channels.ReadableByteChannel

private[ra3] class CharArraySubSeq(buff: Array[Char], start: Int, to: Int)
    extends CharSequence {

  override def length(): Int = to - start

  override def charAt(index: Int): Char = {
    val rel = index + start
    if (rel >= to) throw new IndexOutOfBoundsException
    else buff(rel)
  }

  override def subSequence(start: Int, end: Int): CharSequence = ???

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

}
