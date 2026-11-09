package ra3.ts

import ra3.*
import tasks.*
import tasks.jsonitersupport.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import cats.effect.IO

private[ra3] sealed trait SerializedBuffer
private[ra3] object SerializedBuffer {
  final case class SInt(values: Array[Int]) extends SerializedBuffer
  final case class SLong(values: Array[Long]) extends SerializedBuffer
  final case class SDouble(values: Array[Double]) extends SerializedBuffer
  final case class SInstant(values: Array[Long]) extends SerializedBuffer
  final case class SString(values: Array[String]) extends SerializedBuffer

  def fromBuffer(b: Buffer): SerializedBuffer = b match {
    case x: BufferInt     => SInt(x.toArray)
    case x: BufferLong    => SLong(x.toArray)
    case x: BufferDouble  => SDouble(x.toArray)
    case x: BufferInstant => SInstant(x.toArray)
    case x: BufferString  => SString(x.toArray.map(_.toString))
  }
  def toBuffer(s: SerializedBuffer): Buffer = s match {
    case SInt(v)     => BufferInt(v)
    case SLong(v)    => BufferLong(v)
    case SDouble(v)  => BufferDouble(v)
    case SInstant(v) => BufferInstant(v)
    case SString(v)  => BufferString(v.map(x => (x: CharSequence)))
  }

  // $COVERAGE-OFF$
  implicit val customCodecOfDouble: JsonValueCodec[Double] =
    ra3.Utils.customDoubleCodec
  implicit val codec: JsonValueCodec[SerializedBuffer] = JsonCodecMaker.make
  // $COVERAGE-ON$
}

private[ra3] case class MakeSegmentFromBuffer(
    data: SerializedBuffer,
    outputPath: LogicalPath
)
private[ra3] object MakeSegmentFromBuffer {
  def queue(buffer: Buffer, outputPath: LogicalPath)(implicit
      tsc: ExtendedTaskSystemComponents
  ): IO[Segment] =
    task(MakeSegmentFromBuffer(SerializedBuffer.fromBuffer(buffer), outputPath))(
      ResourceRequest(
        cpu = (1, 1),
        memory = (buffer.length.toLong * 16L / 1000000L).toInt + 100,
        scratch = 0,
        gpu = 0
      )
    )

  // $COVERAGE-OFF$
  implicit val codec: JsonValueCodec[MakeSegmentFromBuffer] = JsonCodecMaker.make
  // $COVERAGE-ON$

  val task = Task[MakeSegmentFromBuffer, Segment]("MakeSegmentFromBuffer", 1) {
    case input =>
      implicit ce =>
        val buffer = SerializedBuffer.toBuffer(input.data)
        val segment: IO[Segment] = buffer match {
          case b: BufferInt => ColumnTag.I32.toSegment(b, input.outputPath)
          case b: BufferLong => ColumnTag.I64.toSegment(b, input.outputPath)
          case b: BufferDouble => ColumnTag.F64.toSegment(b, input.outputPath)
          case b: BufferInstant =>
            ColumnTag.Instant.toSegment(b, input.outputPath)
          case b: BufferString =>
            ColumnTag.StringTag.toSegment(b, input.outputPath)
        }
        segment
  }
}
