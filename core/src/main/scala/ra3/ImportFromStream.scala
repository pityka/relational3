package ra3

import cats.effect.IO
import scala.reflect.ClassTag
import tasks.{TaskSystemComponents, ExtendedTaskSystemComponents}
import java.time.Instant
import ra3.join.*
private object ImportFromStream {
  type M[Tup <: Tuple] <: Tuple = Tup match {
    case EmptyTuple => EmptyTuple
    case h *: t     => BufferOf[h] *: M[t]
  }
  type BufferOf[T] = T match {
    case (String)            => MBufferG[CharSequence]
    case (java.time.Instant) => MBufferLong
    case (Long)              => MBufferLong
    case (Double)            => MBufferDouble
    case (Int)               => MBufferInt
  }
  transparent inline def buildBuffer[T]: BufferOf[T] =
    inline scala.compiletime.erasedValue[T] match {
      case _: String            => MutableBuffer.emptyG[CharSequence]
      case _: java.time.Instant => MutableBuffer.emptyL
      case _: Long              => MutableBuffer.emptyL
      case _: Double            => MutableBuffer.emptyD
      case _: Int               => MutableBuffer.emptyI
    }

  transparent inline def buildBuffers[T <: Tuple]: M[T] =
    inline scala.compiletime.erasedValue[T] match {
      case _: EmptyTuple => EmptyTuple
      case _: (h *: t) =>
        buildBuffer[h] *: buildBuffers[t]

    }

  type M2[T <: Tuple] <: Tuple = (T, M[T]) match {
    case (h *: t, a *: b)         => BufferOf[h] *: M2[t]
    case (EmptyTuple, EmptyTuple) => EmptyTuple
    case (_, EmptyTuple)          => EmptyTuple
    case (EmptyTuple, _)          => EmptyTuple
  }

  inline def append[T](t: T, buffer: BufferOf[T]): BufferOf[T] = {
    inline (t, buffer) match {
      case (x: String, y: BufferOf[String]) => y.+=(x)
      case (x: java.time.Instant, y: BufferOf[java.time.Instant]) =>
        y.+=(x.toEpochMilli())
      case (x: Long, y: BufferOf[Long])     => y.+=(x)
      case (x: Double, y: BufferOf[Double]) => y.+=(x)
      case (x: Int, y: BufferOf[Int])       => y.+=(x)
    }
    buffer
  }

  transparent inline def appendBuffers[T <: Tuple](
      t: T,
      buffers: M[T]
  ): M2[T] = {

    inline (t, buffers) match {
      case abcd: ((h *: t), bh *: bt) =>
        val (hh *: tt, bh *: bt) = abcd
        val x: BufferOf[h] = append[h](hh, bh.asInstanceOf[BufferOf[h]])
        x *: appendBuffers[t](tt, bt.asInstanceOf[M[t]])
      case _: (EmptyTuple, EmptyTuple) => EmptyTuple
      case _: (_, EmptyTuple)          => EmptyTuple
      case _: (EmptyTuple, _)          => EmptyTuple
    }
  }

  private[ra3] def queueSegmentFromBuffer(
      buffer: Buffer,
      path: LogicalPath,
      tsc: ExtendedTaskSystemComponents
  ): IO[Segment] =
    ra3.ts.MakeSegmentFromBuffer.queue(buffer, path)(using tsc)

  transparent inline def columns[T](
      buffer: BufferOf[T],
      tsc: ExtendedTaskSystemComponents,
      path: LogicalPath
  ): IO[TaggedColumn] =
    inline (scala.compiletime.erasedValue[T], buffer) match {
      case (_: String, b: BufferOf[String]) =>
        queueSegmentFromBuffer(BufferString(b.toArray), path, tsc)
          .map(s => Column.StringColumn(Vector(s.asInstanceOf[SegmentString])))
      case (_: java.time.Instant, b: BufferOf[java.time.Instant]) =>
        queueSegmentFromBuffer(BufferInstant(b.toArray), path, tsc)
          .map(s => Column.InstantColumn(Vector(s.asInstanceOf[SegmentInstant])))

      case (_: Long, b: BufferOf[Long]) =>
        queueSegmentFromBuffer(BufferLong(b.toArray), path, tsc)
          .map(s => Column.I64Column(Vector(s.asInstanceOf[SegmentLong])))
      case (_: Double, b: BufferOf[Double]) =>
        queueSegmentFromBuffer(BufferDouble(b.toArray), path, tsc)
          .map(s => Column.F64Column(Vector(s.asInstanceOf[SegmentDouble])))
      case (_: Int, b: BufferOf[Int]) =>
        queueSegmentFromBuffer(BufferIntInArray(b.toArray), path, tsc)
          .map(s => Column.Int32Column(Vector(s.asInstanceOf[SegmentInt])))
    }

  transparent inline def closeBuffers[T <: Tuple](
      buffers: M[T],
      tsc: ExtendedTaskSystemComponents,
      path: LogicalPath,
      i: Int
  ): List[IO[TaggedColumn]] = {
    inline (scala.compiletime.erasedValue[T]) match {
      case _: EmptyTuple => Nil
      case _: (h0 *: t0) =>
        val (h *: t) = buffers.asInstanceOf[BufferOf[h0] *: M[t0]]
        columns[h0](h, tsc, path.copy(column = i)) :: closeBuffers[t0](
          t,
          tsc,
          path,
          i + 1
        )
    }
  }
  type DefToColumnType[T] = T match {
    case Int     => ra3.I32Var
    case Long    => ra3.I64Var
    case Double  => ra3.F64Var
    case String  => ra3.StrVar
    case Instant => ra3.InstVar
  }
  inline def importFromStream[N <: Tuple, T <: Tuple: ClassTag](
      stream: fs2.Stream[IO, scala.NamedTuple.NamedTuple[N, T]],
      uniqueId: String,
      minimumSegmentSize: Int,
      maximumSegmentSize: Int
  )(implicit tsc: ExtendedTaskSystemComponents) = {
    stream
      .chunkMin(minimumSegmentSize)
      .unchunks
      .chunkLimit(maximumSegmentSize)
      .zipWithIndex
      .evalMap { case (ch, idx) =>
        val seq = ch.asSeq
        val path = LogicalPath(
          table = uniqueId,
          partition = None,
          segment = idx.toInt,
          column = -1
        )
        val buffers = buildBuffers[T]
        ch.foreach { t => appendBuffers(t, buffers) }
        val ios = closeBuffers(buffers, tsc, path, 0)
        IO.parSequenceN(ios.size)(ios).map(_.toVector)

      }
      .compile
      .toVector
      .flatMap { v =>
        if (v.nonEmpty)
          IO.pure {
            val columns = v.transpose.map { column =>
              val head = column.head
              val tag = head.tag
              val segments = column
                .flatMap(_.segments)
                .map((_: Segment).asInstanceOf[tag.SegmentType])
              tag.makeTaggedColumn(tag.makeColumn(segments))
            }
            val names = columns.zipWithIndex.map(v => s"${v._2}")
            Table(columns, names, uniqueId, None)
          }
        else {
          val path = LogicalPath(
            table = uniqueId,
            partition = None,
            segment = 0,
            column = -1
          )
          val buffers = buildBuffers[T]
          val ios = closeBuffers(buffers, tsc, path, 0)
          IO.parSequenceN(math.max(1, ios.size))(ios).map { cols =>
            val columns = cols.toVector
            val names = columns.zipWithIndex.map(v => s"${v._2}")
            Table(columns, names, uniqueId, None)
          }
        }
      }
      .map(_.as[N, Tuple.Map[T, DefToColumnType]])

  }
}
