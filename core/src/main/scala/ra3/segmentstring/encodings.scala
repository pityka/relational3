package ra3.segmentstring

import tasks.TaskSystemComponents
import ra3.BufferString
import ra3.Utils
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import cats.effect.IO
import java.nio.ByteBuffer
import java.nio.ByteOrder
import tasks.fileservice.SharedFile
import ra3.logElapsed
import ra3.countInStr
import ra3.ByteBufferAsCharSequence
sealed trait SerializationType {
  def numBytes: Long
  def buffer(implicit tsc: TaskSystemComponents): IO[BufferString]
}
object SerializationType {
  implicit val segmentCodec: JsonValueCodec[SerializationType] =
    JsonCodecMaker.make
}
object PrefixLength {
  def apply(values: Array[CharSequence], name: String)(implicit
      tsc: TaskSystemComponents
  ): IO[PrefixLength] = {
    IO.cede >> (IO {
      val byteSize = values.map(v => (v.length * 2L)).sum
      if (byteSize > Int.MaxValue - 100)
        (
          fs2.Stream.raiseError[IO](
            new RuntimeException(
              "String buffers longer than what fits into an array not implemented"
            )
          ),
          fs2.Stream.raiseError[IO](
            new RuntimeException(
              "String buffers longer than what fits into an array not implemented"
            )
          ),
          -1L
        )
      else {
        val bbData =
          ByteBuffer.allocate(byteSize.toInt).order(ByteOrder.BIG_ENDIAN)
        var numBytes = 0L
        values.foreach { str =>

          numBytes += str.length() * 2 + 20 // for ram usage guess
          var i = 0
          while (i < str.length) {
            bbData.putChar(str.charAt(i))
            i += 1
          }

        }
        bbData.flip()

        val bbCompressed = Utils.compress(bbData)

        val lengths = values.map(_.length)
        val bbLengths =
          ByteBuffer
            .allocate(4 * lengths.length)
            .order(ByteOrder.LITTLE_ENDIAN)
        bbLengths.asIntBuffer().put(lengths)

        (
          fs2.Stream.chunk(fs2.Chunk.byteBuffer(bbCompressed)),
          fs2.Stream.chunk(fs2.Chunk.byteBuffer(Utils.compress(bbLengths))),
          numBytes
        )
      }
    }.flatMap { case (streamData, streamLengths, numBytes) =>
      IO.both(
        SharedFile
          .apply(streamData, name.toString + ".data"),
        SharedFile
          .apply(streamLengths, name.toString + ".lengths")
      ).map { case (sfData, sfLength) =>
        PrefixLength(
          lengths = sfLength,
          bytes = sfData,
          numBytes = numBytes,
          numElems = values.length
        )
      }
    }.logElapsed)
      .guarantee(IO.cede)
  }
}
case class PrefixLength(
    lengths: SharedFile,
    bytes: SharedFile,
    numBytes: Long,
    numElems: Int
) extends SerializationType {
  def buffer(implicit tsc: TaskSystemComponents): IO[BufferString] = {
    IO.cede >> IO
      .both(lengths.bytes, bytes.bytes)
      .map { case (lengths, data) =>
        val decompressedLengths = Utils.decompress(lengths)
        val decompressedData = Utils.decompress(data)

        val bbData =
          decompressedData
            .order(ByteOrder.BIG_ENDIAN) // char data is laid out big endian
        val decodedLengths = {
          decompressedLengths
            .order(ByteOrder.LITTLE_ENDIAN)
            .asIntBuffer()
        }

        assert(decodedLengths.remaining == numElems)
        val ar = Array.ofDim[CharSequence](numElems)
        var i = 0
        while (i < numElems) {
          val len = decodedLengths.get(i)
          val wrap2 =
            ByteBufferAsCharSequence(bbData.slice(bbData.position(), len * 2))
          ar(i) = wrap2
          bbData.position(bbData.position() + len * 2)
          i += 1
        }
        BufferString(ar)
      }
      .logElapsed
      .countInStr(numElems, bytes.byteSize + lengths.byteSize)
      .guarantee(IO.cede)
  }
}
object Dictionary {
  def apply(
      values: Array[CharSequence],
      name: String,
      distincts: Array[CharSequence]
  )(implicit
      tsc: TaskSystemComponents
  ): IO[Dictionary] = {
    IO.cede >> IO {
      val map = ra3.hashtable.CharSequenceTable.buildFirst(distincts, null)
      val ids = values.map(v => map.lookupIdx(v))
      val bb =
        ByteBuffer
          .allocate(4 * ids.length)
          .order(ByteOrder.LITTLE_ENDIAN)
      bb.asIntBuffer().put(ids)
      (
        fs2.Stream.chunk(fs2.Chunk.byteBuffer(Utils.compress(bb))),
        4 * ids.length
      )
    }.flatMap { case (stream, numBytes) =>
      IO.both(SharedFile
        .apply(stream, name.toString+".ids"), PrefixLength(distincts,name+".dictionary"))
        .map { case (sf,dictionary) =>
          Dictionary(
            dictionary = dictionary,
            ids = sf,
            numBytes = numBytes,
            numElems = values.length
          )
        }
    }.logElapsed.guarantee(IO.cede)
  }
  case class Dictionary(
      dictionary: PrefixLength,
      ids: SharedFile,
      numBytes: Long,
      numElems: Int
  ) extends SerializationType {
    def buffer(implicit tsc: TaskSystemComponents): IO[BufferString] = {

      IO.cede >>  IO.both(ids.bytes, dictionary.buffer)
        .map { case (ids,dictionary) =>
          val decompressed = Utils.decompress(ids)

          val decoded = {
            val bb = decompressed
              .order(ByteOrder.LITTLE_ENDIAN)
              .asIntBuffer()
            val ar = Array.ofDim[Int](bb.remaining)
            bb.get(ar)
            ar
          }

          assert(decoded.length == numElems)
          val ar = Array.ofDim[CharSequence](numElems)
          var i = 0
          while (i < numElems) {
            ar(i) = dictionary.values(decoded(i))
            i += 1
          }
          BufferString(ar)
        }
        .logElapsed
        .countInStr(numElems, ids.byteSize)
        .guarantee(IO.cede)

    }
  }
}
