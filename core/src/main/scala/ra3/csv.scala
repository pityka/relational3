package ra3

import java.nio.channels.ReadableByteChannel
import java.nio.charset.CharsetDecoder
import java.io.File
import org.saddle.io.csv.Callback
import tasks.TaskSystemComponents
import scala.collection.mutable.ArrayBuffer
import cats.effect.IO
import ra3.ColumnTag.I32
import ra3.ColumnTag.Instant
import ra3.ColumnTag.F64
import ra3.ColumnTag.I64
import ra3.ColumnTag.StringTag

trait InstantParser {
  def read(buff: Array[Char], start: Int, until: Int): Long
}
object InstantParser {
  val ISO = new InstantParser {
    def read(buff: Array[Char], start: Int, to: Int): Long = {
      val cs = new CharArraySubSeq(buff, start, to)
      java.time.Instant.parse(cs).toEpochMilli()
    }
  }

}

object csv {

  def readHeterogeneousFromCSVFile(
      name: String,
      columnTypes: Seq[(Int, ColumnTag, Option[InstantParser])],
      file: File,
      charset: CharsetDecoder = org.saddle.io.csv.makeAsciiSilentCharsetDecoder,
      fieldSeparator: Char = ',',
      quoteChar: Char = '"',
      recordSeparator: String = "\r\n",
      maxLines: Long = Long.MaxValue,
      maxSegmentLength: Int = 100000,
      header: Boolean = false
  )(implicit
      tsc: TaskSystemComponents
  ) = {
    val fis = new java.io.FileInputStream(file)
    val channel = fis.getChannel
    try {
      readHeterogeneousFromCSVChannel(
        name,
        columnTypes,
        channel,
        charset,
        fieldSeparator,
        quoteChar,
        recordSeparator,
        maxLines,
        maxSegmentLength,
        header
      )
    } finally {
      fis.close
    }
  }
  def readHeterogeneousFromCSVChannel(
      name: String,
      columnTypes: Seq[(Int, ColumnTag, Option[InstantParser])],
      channel: ReadableByteChannel,
      charset: CharsetDecoder = org.saddle.io.csv.makeAsciiSilentCharsetDecoder,
      fieldSeparator: Char = ',',
      quoteChar: Char = '"',
      recordSeparator: String = "\r\n",
      maxLines: Long = Long.MaxValue,
      maxSegmentLength: Int = 1_000_000,
      header: Boolean = false,
      bufferSize: Int = 8192
  )(implicit
      tsc: TaskSystemComponents
  ) = {

    val sortedColumnTypes = columnTypes
      .sortBy(_._1)
      .toArray

    val locs = sortedColumnTypes.map(_._1)

    require(
      locs.distinct.size == locs.size,
      "Column index fields in columnTypes are not unique. "
    )

    val callback = new ColumnBufferCallback(
      name,
      maxLines,
      header,
      sortedColumnTypes,
      maxSegmentLength
    )

    val error: Option[String] = org.saddle.io.csv.parse(
      channel = channel,
      callback = callback,
      bufferSize = bufferSize,
      charset = charset,
      fieldSeparator = fieldSeparator,
      quoteChar = quoteChar,
      recordSeparator = recordSeparator
    )

    error match {
      case Some(error) => Left(error)
      case None =>
        callback.uploadAndResetBufData(true)
        val colIndex = if (header) Some(callback.headerFields) else None

        if (locs.length > 0 && callback.headerLocFields != locs.length) {

          Left(
            s"Header line to short for given locs: ${locs.mkString("[", ", ", "]")}. Header line: ${callback.allHeaderFields
                .mkString("[", ", ", "]")}"
          )
        } else {
          val columns =
            callback.segments.zip(sortedColumnTypes).zipWithIndex map {
              case ((b, (_, tpe, _)), idx) =>
                val segments = b.toVector
                val column = tpe.makeColumn(segments.map(_.as(tpe)))

                val name = colIndex.map(_.apply(idx))
                (name, column)
            }
          Right(
            Table(
              columns.map(_._2).toVector,
              columns
                .map(_._1)
                .zipWithIndex
                .map { case (maybe, idx) => maybe.getOrElse(s"V$idx") }
                .toVector,
              name,
              None
            )
          )

        }
    }
  }

  class ColumnBufferCallback(
      name: String,
      maxLines: Long,
      header: Boolean,
      columnTypes: Array[(Int, ColumnTag, Option[InstantParser])],
      maxSegmentLength: Int
  )(implicit tsc: TaskSystemComponents)
      extends Callback {

    val locs = columnTypes.map(_._1)
    private val locsIdx : org.saddle.Index[Int] = org.saddle.Index(locs)

    val headerFields = scala.collection.mutable.ArrayBuffer[String]()
    val allHeaderFields = scala.collection.mutable.ArrayBuffer[String]()

    var headerAllFields = 0
    var headerLocFields = 0

    def allocateBuffers() = columnTypes.map { ct =>
      ct._2 match {
        case I32       => org.saddle.Buffer.empty[Int]
        case StringTag => org.saddle.Buffer.empty[String]
        case Instant   => org.saddle.Buffer.empty[Long]
        case I64       => org.saddle.Buffer.empty[Long]
        case F64       => org.saddle.Buffer.empty[Double]
      }

    }

    var bufdata: Array[_] = allocateBuffers()

    val types: Array[ColumnTag] = columnTypes.map(_._2)
    val parsers: Array[Option[InstantParser]] = columnTypes.map(_._3)

    val segments: Vector[ArrayBuffer[Segment]] =
      types.map(_ => ArrayBuffer.empty[Segment]).toVector

    private val emptyLoc = locs.length == 0

    private final def add(s: Array[Char], from: Int, to: Int, buf: Int) = {
      val tpe0 = types(buf)
      tpe0 match {
        case I32 =>
          bufdata(buf)
            .asInstanceOf[org.saddle.Buffer[Int]]
            .+=(org.saddle.scalar.ScalarTagInt.parse(s, from, to))
        case Instant =>
          bufdata(buf)
            .asInstanceOf[org.saddle.Buffer[Long]]
            .+=(parsers(buf).getOrElse(InstantParser.ISO).read(s, from, to))
        case F64 =>
          bufdata(buf)
            .asInstanceOf[org.saddle.Buffer[Double]]
            .+=(org.saddle.scalar.ScalarTagDouble.parse(s, from, to))
        case I64 =>
          bufdata(buf)
            .asInstanceOf[org.saddle.Buffer[Long]]
            .+=(org.saddle.scalar.ScalarTagLong.parse(s, from, to))
        case StringTag =>
          bufdata(buf)
            .asInstanceOf[org.saddle.Buffer[String]]
            .+=(String.valueOf(s, from, to - from))
      }

    }

    def uploadAndResetBufData(force: Boolean) = {
      // upload
      val lengths = bufdata.map(_ match {
        case t: org.saddle.Buffer[_] => t.length
      })

      assert(lengths.distinct.size == 1, lengths.toList)
      val length = lengths.head
      if ((force && length > 0) || length == maxSegmentLength) {
        import cats.effect.unsafe.implicits.global

        IO.parSequenceN(32)(bufdata.zipWithIndex.toList.map {
          case (b, bufferIdx) =>
            b match {
              case t: org.saddle.Buffer[_] =>
                val tpe = columnTypes(bufferIdx)._2

                tpe
                  .makeBuffer(
                    t.asInstanceOf[org.saddle.Buffer[tpe.Elem]].toArray
                  )
                  .toSegment(
                    LogicalPath(
                      table = name,
                      partition = None,
                      segment = segments.head.size,
                      column = bufferIdx
                    )
                  )
                  .map(segment => (bufferIdx, segment))

            }

        }).unsafeRunSync()
          .foreach { case (bufferIdx, segment) =>
            synchronized {
              segments(bufferIdx).append(segment)
            }
          }

        bufdata = allocateBuffers()

      }

    }

    private var loc = 0
    private var line = 0L
    def apply(
        s: Array[Char],
        from: Array[Int],
        to: Array[Int],
        len: Int,
        eol: Array[Int]
    ): org.saddle.io.csv.Control = {
      var i = 0

      var error = false
      var errorString = ""

      if (len == -2) {
        error = true
        errorString =
          s"Unclosed quote after line $line (not necessarily in that line)"
      }

      while (i < len && line < maxLines && !error) {
        val fromi = from(i)
        val toi = to(i)
        val ptoi = math.abs(toi)
        if (line == 0) {
          allHeaderFields.append(new String(s, fromi, ptoi - fromi))
          headerAllFields += 1
          if (emptyLoc || locsIdx.contains(loc)) {
            headerLocFields += 1
          }
        }

        if (emptyLoc || locsIdx.contains(loc)) {
          if (header && line == 0) {
            headerFields.append(new String(s, fromi, ptoi - fromi))
          } else {
            if (loc >= headerAllFields) {
              error = true
              errorString =
                s"Too long line ${line + 1} (1-based). Expected $headerAllFields fields, got ${loc + 1}.  "
            } else {
              add(s, fromi, ptoi, locsIdx.getFirst(loc))
            }
          }
        }

        if (toi < 0 || eol(i) < 0) {
          if (line == 0 && !emptyLoc && headerLocFields != locs.length) {
            error = true
            errorString =
              s"Header line to short for given locs: ${locs.mkString("[", ", ", "]")}. Header line: ${allHeaderFields
                  .mkString("[", ", ", "]")}"
          }
          if (loc < headerAllFields - 1) {
            error = true
            errorString =
              s"Too short line ${line + 1} (1-based). Expected $headerAllFields fields, got ${loc + 1}."
          }

          if (!error) {
            uploadAndResetBufData(false)
          }

          loc = 0
          line += 1
        } else loc += 1
        i += 1
      }

      if (error) org.saddle.io.csv.Error(errorString)
      else if (line >= maxLines) org.saddle.io.csv.Done
      else org.saddle.io.csv.Next
    }

  }

}
