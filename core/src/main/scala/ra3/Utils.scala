package ra3
import java.nio.ByteBuffer
import java.nio.channels.WritableByteChannel
import java.nio.channels.ReadableByteChannel

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