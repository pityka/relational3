package ra3

import java.nio.channels.ReadableByteChannel
import java.nio.charset.CharsetDecoder
import java.io.File
import java.nio.channels.WritableByteChannel
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import org.saddle.io.csv.Callback
import tasks.TaskSystemComponents
import scala.collection.mutable.ArrayBuffer
import cats.effect.IO
import tasks.fileservice.SharedFile

object csv {

  def writeCSVToSharedFiles(
      table: Table,
      channel: WritableByteChannel,
      columnSeparator: Char = ',',
      quoteChar: Char = '"',
      recordSeparator: String = "\r\n",
      charset: Charset = StandardCharsets.UTF_8
  )(implicit tsc: TaskSystemComponents): IO[Seq[SharedFile]] = {

    def toStringColumns(segmentIdx: Int): IO[Seq[Seq[String]]] = {
      IO.parSequenceN(32)((0 until table.columns.size).toList map { colIdx =>
        val column = table.columns(colIdx)
        column match {
          case c: Column.Int32Column =>
            val m =
              c
                .segments(segmentIdx)
                .buffer
                .map(_.toSeq.map(_.toString))

            m

        }
      })
    }

    def quote(s: String) =
      if (s.contains(columnSeparator)) s"$quoteChar$s$quoteChar" else s

    val columnSeparatorStr = columnSeparator.toString
    channel.write(
      ByteBuffer.wrap(
        (table.colNames.toSeq
          .map(quote)
          .mkString(columnSeparatorStr)
          + recordSeparator)
          .getBytes(charset)
      )
    )

    val segments = table.columns.head.segments.size

    IO.parSequenceN(32)(((0 until segments).toList).map { segmentIdx =>
      val data = toStringColumns(segmentIdx).map(_.transpose).flatMap { rows =>
        val renderedBatch = rows.map { row =>
          row.map(quote).mkString(columnSeparatorStr) + recordSeparator
        }.mkString
        val asByte = renderedBatch.getBytes(charset)

        SharedFile.apply(asByte, s"${table.uniqueId}/csv/$segmentIdx.csv")
      }

      data

    })

  }
  def readHeterogeneousFromCSVFile(
      name: String,
      columnTypes: Seq[(Int, ColumnTag)],
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
      columnTypes: Seq[(Int, ColumnTag)],
      channel: ReadableByteChannel,
      charset: CharsetDecoder = org.saddle.io.csv.makeAsciiSilentCharsetDecoder,
      fieldSeparator: Char = ',',
      quoteChar: Char = '"',
      recordSeparator: String = "\r\n",
      maxLines: Long = Long.MaxValue,
      maxSegmentLength: Int = 100000,
      header: Boolean = false,
      bufferSize: Int = 8192
  )(implicit
      tsc: TaskSystemComponents
  ) = {

    val sortedColumnTypes = columnTypes
      .sortBy(_._1)
      .toArray

    val locs = sortedColumnTypes.map(_._1)

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
        val colIndex = if (header) Some(callback.headerFields) else None

        if (locs.length > 0 && callback.headerLocFields != locs.length) {

          Left(
            s"Header line to short for given locs: ${locs.mkString("[", ", ", "]")}. Header line: ${callback.allHeaderFields
                .mkString("[", ", ", "]")}"
          )
        } else {
          callback.uploadAndResetBufData()
          val columns =
            callback.segments.zip(sortedColumnTypes).zipWithIndex map {
              case ((b, (_, tpe)), idx) =>
                val segments = b.toVector
                val column = tpe match {
                  case tpe:ColumnTag.I32.type => Column.Int32Column(tpe.cast(segments))
                  case tpe:ColumnTag.F64.type => Column.F64Column(tpe.cast(segments))
                }

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
              name
            )
          )

        }
    }
  }

  class ColumnBufferCallback(
      name: String,
      maxLines: Long,
      header: Boolean,
      columnTypes: Array[(Int, ColumnTag)],
      maxSegmentLength: Int
  )(implicit tsc: TaskSystemComponents)
      extends Callback {

    val locs = columnTypes.map(_._1)
    private val locsIdx = org.saddle.Index(locs)

    val headerFields = scala.collection.mutable.ArrayBuffer[String]()
    val allHeaderFields = scala.collection.mutable.ArrayBuffer[String]()

    var headerAllFields = 0
    var headerLocFields = 0

    def allocateBuffers() = columnTypes.map { case (_, tpe) =>
      org.saddle.Buffer.empty[Int]
    }

    var bufdata: Array[_] = allocateBuffers()

    val types: Array[ColumnTag] = columnTypes.map(_._2)

    val segments: Vector[ArrayBuffer[Segment]] =
      types.map(_ => ArrayBuffer.empty[Segment]).toVector

    private val emptyLoc = locs.length == 0

    private final def add(s: Array[Char], from: Int, to: Int, buf: Int) = {
      // val tpe0 = types(buf)
      val buf0 = bufdata(buf).asInstanceOf[org.saddle.Buffer[Int]]

      buf0.+=(org.saddle.scalar.ScalarTagInt.parse(s, from, to))

    }

    def uploadAndResetBufData() = {
      // upload
      val lengths = bufdata.map(_ match {
        case t: org.saddle.Buffer[_] => t.length
      })

      assert(lengths.distinct.size == 1)
      val length = lengths.head
      if (length == maxSegmentLength) {
        import cats.effect.unsafe.implicits.global

        IO.parSequenceN(32)(bufdata.zipWithIndex.toList.map {
          case (b, bufferIdx) =>
            b match {
              case t: org.saddle.Buffer[_] =>
                val tpe = columnTypes(bufferIdx)._2

                tpe match {
                  case tpe: ColumnTag.I32.type =>
                    val b = t.asInstanceOf[org.saddle.Buffer[Int]]
                    tpe.makeBuffer(b.toArray)
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
            }

        }).unsafeRunSync()
          .foreach { case (bufferIdx, segment) =>
            segments(bufferIdx).append(segment)
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
        len: Int
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
                s"Too long line ${line + 1} (1-based). Expected $headerAllFields fields, got ${loc + 1}."
            } else {
              add(s, fromi, ptoi, loc)
            }
          }
        }

        if (toi < 0) {
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

          uploadAndResetBufData()

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
