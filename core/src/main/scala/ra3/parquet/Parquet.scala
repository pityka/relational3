package ra3.parquet

import java.io.ByteArrayOutputStream
import java.nio.{ByteBuffer, ByteOrder}
import io.airlift.compress.snappy.{SnappyCompressor, SnappyDecompressor}
import io.airlift.compress.zstd.ZstdDecompressor
import tasks.{SharedFile, TaskSystemComponents}
import cats.effect.IO

private[ra3] final case class ParquetSegmentRef(
    file: SharedFile,
    ptype: Int,
    codec: Int,
    numValues: Long,
    dataPageOffset: Long,
    dictPageOffset: Long,
    totalCompressed: Long,
    optional: Boolean
) {
  def decode(implicit tsc: TaskSystemComponents): IO[Vector[Option[Any]]] = {
    val start = if (dictPageOffset >= 0) dictPageOffset else dataPageOffset
    file
      .stream(start)
      .take(totalCompressed)
      .compile
      .toVector
      .map { v =>
        val chunk = v.toArray
        Parquet.decodeChunkAt(
          chunk,
          0,
          chunk.length,
          codec,
          ptype,
          optional,
          numValues
        )
      }
  }
}

private[ra3] final class Compact {
  private val buf = new ByteArrayOutputStream()
  def bytes: Array[Byte] = buf.toByteArray
  def size: Int = buf.size

  def writeByte(b: Int): Unit = buf.write(b & 0xff)
  def writeRaw(a: Array[Byte]): Unit = buf.write(a, 0, a.length)

  def writeVarint(v: Long): Unit = {
    var x = v
    while ((x & ~0x7fL) != 0L) {
      writeByte(((x & 0x7f) | 0x80).toInt)
      x >>>= 7
    }
    writeByte((x & 0x7f).toInt)
  }

  def zz64(n: Long): Long = (n << 1) ^ (n >> 63)
  def zz32(n: Int): Long = (((n << 1) ^ (n >> 31)).toLong) & 0xffffffffL

  val T_I32 = 5
  val T_I64 = 6
  val T_DOUBLE = 7
  val T_BINARY = 8
  val T_LIST = 9
  val T_STRUCT = 12

  def field(id: Int, ctype: Int): Unit = {
    writeByte(ctype); writeVarint(zz64(id.toLong))
  }
  def i32(id: Int, v: Int): Unit = { field(id, T_I32); writeVarint(zz32(v)) }
  def i64(id: Int, v: Long): Unit = { field(id, T_I64); writeVarint(zz64(v)) }
  def binary(id: Int, v: Array[Byte]): Unit = {
    field(id, T_BINARY); writeVarint(v.length.toLong); writeRaw(v)
  }
  def str(id: Int, s: String): Unit = binary(id, s.getBytes("UTF-8"))
  def structField(id: Int)(body: Compact => Unit): Unit = {
    field(id, T_STRUCT); body(this); stop()
  }
  def listBegin(id: Int, size: Int, elemType: Int): Unit = {
    field(id, T_LIST)
    if (size <= 14) writeByte((size << 4) | elemType)
    else { writeByte(0xf0 | elemType); writeVarint(size.toLong) }
  }
  def elemI32(v: Int): Unit = writeVarint(zz32(v))
  def elemStr(s: String): Unit = {
    val b = s.getBytes("UTF-8"); writeVarint(b.length.toLong); writeRaw(b)
  }
  def elemStruct(body: Compact => Unit): Unit = { body(this); stop() }
  def stop(): Unit = writeByte(0)
}

private[ra3] final class CReader(val data: Array[Byte]) {
  var pos = 0
  def u8(): Int = { val b = data(pos) & 0xff; pos += 1; b }
  def readVarint(): Long = {
    var shift = 0; var result = 0L; var cont = true
    while (cont) {
      val b = u8()
      result |= (b & 0x7fL) << shift
      if ((b & 0x80) == 0) cont = false else shift += 7
    }
    result
  }
  def zzDecode(v: Long): Long = (v >>> 1) ^ -(v & 1L)
  def readI32(): Int = zzDecode(readVarint()).toInt
  def readI64(): Long = zzDecode(readVarint())
  def readBinary(): Array[Byte] = {
    val len = readVarint().toInt
    val a = java.util.Arrays.copyOfRange(data, pos, pos + len); pos += len; a
  }
  def le32At(p: Int): Int =
    (data(p) & 0xff) | ((data(p + 1) & 0xff) << 8) | ((data(p + 2) & 0xff) << 16) | ((data(p + 3) & 0xff) << 24)

  def fieldHeader(lastId: Int): (Int, Int) = {
    val b = u8()
    if (b == 0) (0, 0)
    else {
      val t = b & 0x0f
      val delta = (b & 0xf0) >>> 4
      val id = if (delta != 0) lastId + delta else zzDecode(readVarint()).toInt
      (t, id)
    }
  }
  def listHeader(): (Int, Int) = {
    val b = u8()
    val et = b & 0x0f
    val szHi = (b & 0xf0) >>> 4
    val sz = if (szHi == 0x0f) readVarint().toInt else szHi
    (sz, et)
  }
  def skip(t: Int): Unit = t match {
    case 1 | 2     => ()
    case 3         => pos += 1
    case 4 | 5 | 6 => readVarint(); ()
    case 7         => pos += 8
    case 8         => val l = readVarint().toInt; pos += l
    case 9 | 10 =>
      val (sz, et) = listHeader(); var i = 0; while (i < sz) { skip(et); i += 1 }
    case 12 => skipStruct()
    case other =>
      throw new RuntimeException(s"cannot skip compact type $other")
  }
  def skipStruct(): Unit = {
    var last = 0; var done = false
    while (!done) {
      val (t, id) = fieldHeader(last)
      if (t == 0) done = true else { last = id; skip(t) }
    }
  }
}

private[ra3] final case class Column(
    name: String,
    ptype: Int,
    convertedType: Option[Int],
    optional: Boolean,
    numRows: Int,
    nonNull: Int,
    defLevels: Array[Int],
    plainValues: Array[Byte],
    minBytes: Option[Array[Byte]],
    maxBytes: Option[Array[Byte]]
)

private[ra3] final case class ReadColumn(
    name: String,
    ptype: Int,
    convertedType: Int,
    values: Vector[Option[Any]]
)

private[ra3] object Parquet {
  val TYPE_INT32 = 1
  val TYPE_INT64 = 2
  val TYPE_FLOAT = 4
  val TYPE_DOUBLE = 5
  val TYPE_BYTE_ARRAY = 6

  val CONV_UTF8 = 0
  val CONV_TIMESTAMP_MILLIS = 9

  val REP_REQUIRED = 0
  val REP_OPTIONAL = 1

  val ENC_PLAIN = 0
  val ENC_RLE = 3

  val CODEC_UNCOMPRESSED = 0
  val CODEC_SNAPPY = 1
  val CODEC_GZIP = 2
  val CODEC_ZSTD = 6
  val PAGE_DATA = 0
  val PAGE_DICTIONARY = 2
  val ENC_PLAIN_DICTIONARY = 2
  val ENC_RLE_DICTIONARY = 8
  val REP_REPEATED = 2
  val MAGIC = "PAR1".getBytes("US-ASCII")

  def snappyCompress(in: Array[Byte]): Array[Byte] = {
    val c = new SnappyCompressor()
    val out = new Array[Byte](c.maxCompressedLength(in.length))
    val w = c.compress(in, 0, in.length, out, 0, out.length)
    java.util.Arrays.copyOf(out, w)
  }
  def snappyDecompress(in: Array[Byte], uncompressedLen: Int): Array[Byte] = {
    val out = new Array[Byte](uncompressedLen)
    new SnappyDecompressor().decompress(in, 0, in.length, out, 0, uncompressedLen)
    out
  }

  def le32(v: Int): Array[Byte] =
    ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(v).array()

  def plainI32(d: Array[Int]): Array[Byte] = {
    val bb = ByteBuffer.allocate(d.length * 4).order(ByteOrder.LITTLE_ENDIAN)
    var i = 0; while (i < d.length) { bb.putInt(d(i)); i += 1 }; bb.array()
  }
  def plainI64(d: Array[Long]): Array[Byte] = {
    val bb = ByteBuffer.allocate(d.length * 8).order(ByteOrder.LITTLE_ENDIAN)
    var i = 0; while (i < d.length) { bb.putLong(d(i)); i += 1 }; bb.array()
  }
  def plainF64(d: Array[Double]): Array[Byte] = {
    val bb = ByteBuffer.allocate(d.length * 8).order(ByteOrder.LITTLE_ENDIAN)
    var i = 0; while (i < d.length) { bb.putDouble(d(i)); i += 1 }; bb.array()
  }
  def plainBytes(d: Array[Array[Byte]]): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    var i = 0
    while (i < d.length) { out.write(le32(d(i).length)); out.write(d(i)); i += 1 }
    out.toByteArray
  }

  def rleBitpackLevels1(levels: Array[Int]): Array[Byte] = {
    val n = levels.length
    val numGroups = (n + 7) / 8
    val header = (numGroups.toLong << 1) | 1L
    val packed = new Array[Byte](numGroups)
    var i = 0
    while (i < n) {
      if (levels(i) != 0) packed(i / 8) = (packed(i / 8) | (1 << (i % 8))).toByte
      i += 1
    }
    val hdr = new Compact(); hdr.writeVarint(header)
    hdr.bytes ++ packed
  }

  private def columnChunkBytes(col: Column, codec: Int): (Array[Byte], Int, Int) = {
    val body = new ByteArrayOutputStream()
    if (col.optional) {
      val lv = rleBitpackLevels1(col.defLevels)
      body.write(le32(lv.length))
      body.write(lv)
    }
    body.write(col.plainValues)
    val pageBody = body.toByteArray
    val storedBody =
      if (codec == CODEC_SNAPPY) snappyCompress(pageBody) else pageBody

    val ph = new Compact()
    ph.i32(1, PAGE_DATA)
    ph.i32(2, pageBody.length)
    ph.i32(3, storedBody.length)
    ph.structField(5) { c =>
      c.i32(1, col.numRows)
      c.i32(2, ENC_PLAIN)
      c.i32(3, ENC_RLE)
      c.i32(4, ENC_RLE)
    }
    ph.stop()
    val chunk = new ByteArrayOutputStream()
    chunk.write(ph.bytes)
    chunk.write(storedBody)
    val cb = chunk.toByteArray
    (cb, ph.bytes.length + pageBody.length, cb.length)
  }

  def writeFile(cols: Seq[Column], numRows: Int, codec: Int): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    out.write(MAGIC)
    var offset = MAGIC.length
    val offsets = scala.collection.mutable.ArrayBuffer.empty[Int]
    val chunkLens = scala.collection.mutable.ArrayBuffer.empty[Int]
    val uncompTotals = scala.collection.mutable.ArrayBuffer.empty[Int]
    cols.foreach { col =>
      val (cb, unc, comp) = columnChunkBytes(col, codec)
      offsets += offset
      chunkLens += comp
      uncompTotals += unc
      out.write(cb)
      offset += comp
    }

    val fm = new Compact()
    fm.i32(1, 2)
    fm.listBegin(2, cols.size + 1, fm.T_STRUCT)
    fm.elemStruct { c => c.str(4, "schema"); c.i32(5, cols.size) }
    cols.foreach { col =>
      fm.elemStruct { c =>
        c.i32(1, col.ptype)
        c.i32(3, if (col.optional) REP_OPTIONAL else REP_REQUIRED)
        c.str(4, col.name)
        col.convertedType.foreach(ct => c.i32(6, ct))
      }
    }
    fm.i64(3, numRows.toLong)
    fm.listBegin(4, 1, fm.T_STRUCT)
    fm.elemStruct { rg =>
      rg.listBegin(1, cols.size, rg.T_STRUCT)
      cols.zipWithIndex.foreach { case (col, i) =>
        rg.elemStruct { cc =>
          cc.i64(2, offsets(i).toLong)
          cc.structField(3) { md =>
            md.i32(1, col.ptype)
            val encs =
              if (col.optional) List(ENC_PLAIN, ENC_RLE) else List(ENC_PLAIN)
            md.listBegin(2, encs.size, md.T_I32)
            encs.foreach(md.elemI32)
            md.listBegin(3, 1, md.T_BINARY)
            md.elemStr(col.name)
            md.i32(4, codec)
            md.i64(5, numRows.toLong)
            md.i64(6, uncompTotals(i).toLong)
            md.i64(7, chunkLens(i).toLong)
            md.i64(9, offsets(i).toLong)
            val nc =
              if (col.optional) Some((numRows - col.nonNull).toLong) else None
            if (nc.isDefined || col.minBytes.isDefined || col.maxBytes.isDefined)
              md.structField(12) { st =>
                nc.foreach(c => st.i64(3, c))
                col.maxBytes.foreach(b => st.binary(5, b))
                col.minBytes.foreach(b => st.binary(6, b))
              }
          }
        }
      }
      rg.i64(2, chunkLens.sum.toLong)
      rg.i64(3, numRows.toLong)
    }
    fm.str(6, "ra3")
    fm.stop()
    val footer = fm.bytes
    out.write(footer)
    out.write(le32(footer.length))
    out.write(MAGIC)
    out.toByteArray
  }

  private def readRleBitpack(
      r: CReader,
      bitWidth: Int,
      count: Int,
      endPos: Int
  ): Array[Int] = {
    val out = new Array[Int](count)
    var produced = 0
    val byteWidth = (bitWidth + 7) / 8
    while (produced < count && r.pos < endPos) {
      val header = r.readVarint().toInt
      if ((header & 1) == 1) {
        val numGroups = header >>> 1
        val nbytes = numGroups * bitWidth
        val base = r.pos
        val total = numGroups * 8
        var k = 0; var bitPos = 0
        while (k < total && produced < count) {
          var v = 0; var b = 0
          while (b < bitWidth) {
            val bit = (r.data(base + (bitPos >> 3)) >> (bitPos & 7)) & 1
            v |= (bit << b); bitPos += 1; b += 1
          }
          out(produced) = v; produced += 1; k += 1
        }
        r.pos = base + nbytes
      } else {
        val repeat = header >>> 1
        var v = 0; var i = 0
        while (i < byteWidth) {
          v |= (r.data(r.pos) & 0xff) << (8 * i); r.pos += 1; i += 1
        }
        var k = 0
        while (k < repeat && produced < count) {
          out(produced) = v; produced += 1; k += 1
        }
      }
    }
    out
  }

  private def gzipDecompress(in: Array[Byte]): Array[Byte] = {
    val gis =
      new java.util.zip.GZIPInputStream(new java.io.ByteArrayInputStream(in))
    try gis.readAllBytes()
    finally gis.close()
  }
  private def zstdDecompress(in: Array[Byte], uncompressedLen: Int): Array[Byte] = {
    val out = new Array[Byte](uncompressedLen)
    new ZstdDecompressor().decompress(in, 0, in.length, out, 0, uncompressedLen)
    out
  }

  private def decompress(
      body: Array[Byte],
      codec: Int,
      uncompressedLen: Int
  ): Array[Byte] = codec match {
    case CODEC_UNCOMPRESSED => body
    case CODEC_SNAPPY       => snappyDecompress(body, uncompressedLen)
    case CODEC_GZIP         => gzipDecompress(body)
    case CODEC_ZSTD         => zstdDecompress(body, uncompressedLen)
    case other =>
      throw new RuntimeException(
        s"Unsupported Parquet compression codec $other. Supported: UNCOMPRESSED, SNAPPY, GZIP, ZSTD."
      )
  }

  private def plainValues(r: CReader, ptype: Int, n: Int): Array[Any] = {
    val out = new Array[Any](n)
    var i = 0
    while (i < n) {
      out(i) = ptype match {
        case TYPE_INT32 => val v = r.le32At(r.pos); r.pos += 4; v
        case TYPE_INT64 =>
          var v = 0L; var k = 0
          while (k < 8) { v |= (r.data(r.pos) & 0xffL) << (8 * k); r.pos += 1; k += 1 }
          v
        case TYPE_FLOAT =>
          var bits = 0; var k = 0
          while (k < 4) { bits |= (r.data(r.pos) & 0xff) << (8 * k); r.pos += 1; k += 1 }
          java.lang.Float.intBitsToFloat(bits).toDouble
        case TYPE_DOUBLE =>
          var bits = 0L; var k = 0
          while (k < 8) {
            bits |= (r.data(r.pos) & 0xffL) << (8 * k); r.pos += 1; k += 1
          }
          java.lang.Double.longBitsToDouble(bits)
        case TYPE_BYTE_ARRAY =>
          val len = r.le32At(r.pos); r.pos += 4
          val a = java.util.Arrays.copyOfRange(r.data, r.pos, r.pos + len)
          r.pos += len; new String(a, "UTF-8")
        case other =>
          throw new RuntimeException(s"unsupported plain type $other")
      }
      i += 1
    }
    out
  }

  final case class Footer(
      rowGroups: Vector[Vector[PColMeta]],
      rowGroupRows: Vector[Int]
  )

  def readFooter(bytes: Array[Byte]): Footer = {
    val footerLen = (bytes(bytes.length - 8) & 0xff) |
      ((bytes(bytes.length - 7) & 0xff) << 8) |
      ((bytes(bytes.length - 6) & 0xff) << 16) |
      ((bytes(bytes.length - 5) & 0xff) << 24)
    val footerStart = bytes.length - 8 - footerLen
    val fr = new CReader(bytes); fr.pos = footerStart

    var schema = Vector.empty[(Int, Int, Int, String)]
    val rowGroups =
      scala.collection.mutable.ArrayBuffer.empty[Vector[PColMeta]]
    val rowGroupRows = scala.collection.mutable.ArrayBuffer.empty[Int]
    var last = 0; var done = false
    while (!done) {
      val (t, id) = fr.fieldHeader(last)
      if (t == 0) done = true
      else {
        last = id
        id match {
          case 2 =>
            val (sz, _) = fr.listHeader()
            var i = 0
            while (i < sz) {
              var tp = -1; var rep = -1; var conv = -1; var nm = ""
              var l2 = 0; var d2 = false
              while (!d2) {
                val (ft, fid) = fr.fieldHeader(l2)
                if (ft == 0) d2 = true
                else {
                  l2 = fid
                  fid match {
                    case 1 => tp = fr.readI32()
                    case 3 => rep = fr.readI32()
                    case 4 => nm = new String(fr.readBinary(), "UTF-8")
                    case 6 => conv = fr.readI32()
                    case _ => fr.skip(ft)
                  }
                }
              }
              schema = schema :+ (tp, rep, conv, nm)
              i += 1
            }
          case 4 =>
            val (sz, _) = fr.listHeader()
            var i = 0
            while (i < sz) {
              val cols = scala.collection.mutable.ArrayBuffer.empty[PColMeta]
              var rgRows = 0
              var l2 = 0; var d2 = false
              while (!d2) {
                val (ft, fid) = fr.fieldHeader(l2)
                if (ft == 0) d2 = true
                else {
                  l2 = fid
                  fid match {
                    case 3 => rgRows = fr.readI64().toInt
                    case 1 =>
                      val (csz, _) = fr.listHeader()
                      var j = 0
                      while (j < csz) {
                        var nm = ""; var tp = -1; var codec = 0; var nv = 0L
                        var dpo = 0L; var dicto = -1L; var tc = 0L
                        var nullCount = -1L
                        var minV: Option[Array[Byte]] = None
                        var maxV: Option[Array[Byte]] = None
                        var minD: Option[Array[Byte]] = None
                        var maxD: Option[Array[Byte]] = None
                        var l3 = 0; var d3 = false
                        while (!d3) {
                          val (ct, cid) = fr.fieldHeader(l3)
                          if (ct == 0) d3 = true
                          else {
                            l3 = cid
                            cid match {
                              case 3 =>
                                var l4 = 0; var d4 = false
                                while (!d4) {
                                  val (mt, mid) = fr.fieldHeader(l4)
                                  if (mt == 0) d4 = true
                                  else {
                                    l4 = mid
                                    mid match {
                                      case 1 => tp = fr.readI32()
                                      case 3 =>
                                        val (psz, _) = fr.listHeader()
                                        var k = 0
                                        while (k < psz) {
                                          nm = new String(fr.readBinary(), "UTF-8")
                                          k += 1
                                        }
                                        if (psz > 1)
                                          throw new RuntimeException(
                                            s"Parquet column with nested/dotted path (depth $psz, leaf '$nm') is not supported; only flat columns are supported."
                                          )
                                      case 4  => codec = fr.readI32()
                                      case 5  => nv = fr.readI64()
                                      case 9  => dpo = fr.readI64()
                                      case 11 => dicto = fr.readI64()
                                      case 7  => tc = fr.readI64()
                                      case 12 =>
                                        var l5 = 0; var d5 = false
                                        while (!d5) {
                                          val (st, sid) = fr.fieldHeader(l5)
                                          if (st == 0) d5 = true
                                          else {
                                            l5 = sid
                                            sid match {
                                              case 1 => maxD = Some(fr.readBinary())
                                              case 2 => minD = Some(fr.readBinary())
                                              case 3 => nullCount = fr.readI64()
                                              case 5 => maxV = Some(fr.readBinary())
                                              case 6 => minV = Some(fr.readBinary())
                                              case _ => fr.skip(st)
                                            }
                                          }
                                        }
                                      case _  => fr.skip(mt)
                                    }
                                  }
                                }
                              case _ => fr.skip(ct)
                            }
                          }
                        }
                        val sc = schema.find(_._4 == nm)
                        val convFor = sc.map(_._3).getOrElse(-1)
                        val repFor = sc.map(_._2).getOrElse(0)
                        if (repFor == REP_REPEATED)
                          throw new RuntimeException(
                            s"Column $nm uses REPEATED (nested/list) schema, which is not supported."
                          )
                        cols += PColMeta(nm, tp, convFor, codec, nv, dpo, dicto, tc,
                          repFor == REP_OPTIONAL, nullCount, minV.orElse(minD), maxV.orElse(maxD))
                        j += 1
                      }
                    case _ => fr.skip(ft)
                  }
                }
              }
              rowGroups += cols.toVector
              rowGroupRows += rgRows
              i += 1
            }
          case _ => fr.skip(t)
        }
      }
    }

    Footer(rowGroups.toVector, rowGroupRows.toVector)
  }

  def decodeChunk(bytes: Array[Byte], cm: PColMeta): Vector[Option[Any]] = {
    val start =
      (if (cm.dictPageOffset >= 0) cm.dictPageOffset else cm.dataPageOffset).toInt
    decodeChunkAt(bytes, start, start + cm.totalCompressed.toInt, cm.codec,
      cm.ptype, cm.optional, cm.numValues)
  }

  def decodeChunkAt(
      bytes: Array[Byte],
      start: Int,
      endOfChunk: Int,
      codec: Int,
      ptype: Int,
      optional: Boolean,
      numValues: Long
  ): Vector[Option[Any]] = {
    val r = new CReader(bytes); r.pos = start
    var dict: Array[Any] = null
    val collected = scala.collection.mutable.ArrayBuffer.empty[Option[Any]]
    var remaining = numValues
    while (r.pos < endOfChunk && (remaining > 0 || dict == null)) {
          var pType = -1; var ups = 0; var cps = 0; var dpNum = 0; var dpEnc = 0
          var dictNum = 0
          var l = 0; var d = false
          while (!d) {
            val (t, id) = r.fieldHeader(l)
            if (t == 0) d = true
            else {
              l = id
              id match {
                case 1 => pType = r.readI32()
                case 2 => ups = r.readI32()
                case 3 => cps = r.readI32()
                case 5 =>
                  var l2 = 0; var d2 = false
                  while (!d2) {
                    val (ft, fid) = r.fieldHeader(l2)
                    if (ft == 0) d2 = true
                    else {
                      l2 = fid
                      fid match {
                        case 1 => dpNum = r.readI32()
                        case 2 => dpEnc = r.readI32()
                        case _ => r.skip(ft)
                      }
                    }
                  }
                case 7 =>
                  var l2 = 0; var d2 = false
                  while (!d2) {
                    val (ft, fid) = r.fieldHeader(l2)
                    if (ft == 0) d2 = true
                    else {
                      l2 = fid
                      fid match {
                        case 1 => dictNum = r.readI32()
                        case _ => r.skip(ft)
                      }
                    }
                  }
                case _ => r.skip(t)
              }
            }
          }
          val pageData = java.util.Arrays.copyOfRange(bytes, r.pos, r.pos + cps)
          r.pos += cps
          val body = decompress(pageData, codec, ups)
          if (pType == PAGE_DICTIONARY) {
            val dr = new CReader(body)
            dict = plainValues(dr, ptype, dictNum)
          } else if (pType == PAGE_DATA) {
            val dr = new CReader(body)
            val maxDef = if (optional) 1 else 0
            val defLevels =
              if (maxDef > 0) {
                val len = dr.le32At(dr.pos); dr.pos += 4
                val end = dr.pos + len
                val dl = readRleBitpack(dr, 1, dpNum, end)
                dr.pos = end
                dl
              } else Array.fill(dpNum)(1)
            val present = defLevels.count(_ == 1)
            val vals: Array[Any] =
              if (dpEnc == ENC_PLAIN) plainValues(dr, ptype, present)
              else if (dpEnc == ENC_PLAIN_DICTIONARY || dpEnc == ENC_RLE_DICTIONARY) {
                if (dict == null)
                  throw new RuntimeException(
                    "dictionary-encoded data page without a preceding dictionary page"
                  )
                val bw = dr.u8()
                val idx = readRleBitpack(dr, bw, present, body.length)
                idx.map(ix => dict(ix))
              } else
                throw new RuntimeException(
                  s"Unsupported Parquet data page encoding $dpEnc. Supported: PLAIN, PLAIN_DICTIONARY, RLE_DICTIONARY. DELTA_* and BYTE_STREAM_SPLIT are not supported."
                )
            var vi = 0; var di = 0
            while (di < dpNum) {
              if (defLevels(di) == 1) { collected += Some(vals(vi)); vi += 1 }
              else collected += None
              di += 1
            }
            remaining -= dpNum
          } else
            throw new RuntimeException(
              s"Unsupported Parquet page type $pType. DATA_PAGE_V2 and index pages are not supported."
            )
    }
    collected.toVector
  }

  def readFile(bytes: Array[Byte]): Vector[ReadColumn] = {
    val footer = readFooter(bytes)
    footer.rowGroups.head.map { headCm =>
      val values = footer.rowGroups.flatMap(rg =>
        decodeChunk(bytes, rg.find(_.name == headCm.name).get)
      )
      ReadColumn(headCm.name, headCm.ptype, headCm.convertedType, values)
    }.toVector
  }

  def statValue(ptype: Int, bytes: Array[Byte]): Any = ptype match {
    case TYPE_INT32 =>
      (bytes(0) & 0xff) | ((bytes(1) & 0xff) << 8) |
        ((bytes(2) & 0xff) << 16) | ((bytes(3) & 0xff) << 24)
    case TYPE_INT64 =>
      var v = 0L; var k = 0
      while (k < 8) { v |= (bytes(k) & 0xffL) << (8 * k); k += 1 }
      v
    case TYPE_FLOAT =>
      var bits = 0; var k = 0
      while (k < 4) { bits |= (bytes(k) & 0xff) << (8 * k); k += 1 }
      java.lang.Float.intBitsToFloat(bits).toDouble
    case TYPE_DOUBLE =>
      var b = 0L; var k = 0
      while (k < 8) { b |= (bytes(k) & 0xffL) << (8 * k); k += 1 }
      java.lang.Double.longBitsToDouble(b)
    case TYPE_BYTE_ARRAY => new String(bytes, "UTF-8")
    case other =>
      throw new RuntimeException(s"unsupported statistic type $other")
  }
}

private[ra3] final case class PColMeta(
    name: String,
    ptype: Int,
    convertedType: Int,
    codec: Int,
    numValues: Long,
    dataPageOffset: Long,
    dictPageOffset: Long,
    totalCompressed: Long,
    optional: Boolean,
    nullCount: Long,
    minBytes: Option[Array[Byte]],
    maxBytes: Option[Array[Byte]]
)
