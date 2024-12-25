package ra3

import cats.effect.IO
import scala.reflect.ClassTag
import tasks.TaskSystemComponents
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

  transparent inline def columns[T](
      buffer: BufferOf[T],
      tsc: TaskSystemComponents,
      path: LogicalPath
  ): IO[TaggedColumn] =
    inline (scala.compiletime.erasedValue[T], buffer) match {
      case (_: String, b: BufferOf[String]) =>
        BufferString(b.toArray)
          .toSegment(name = path)(using tsc)
          .map(s => Column.StringColumn(Vector(s)))
      case (_: java.time.Instant, b: BufferOf[java.time.Instant]) =>
        BufferInstant(b.toArray)
          .toSegment(name = path)(using tsc)
          .map(s => Column.InstantColumn(Vector(s)))

      case (_: Long, b: BufferOf[Long]) =>
        BufferLong(b.toArray)
          .toSegment(name = path)(using tsc)
          .map(s => Column.I64Column(Vector(s)))
      case (_: Double, b: BufferOf[Double]) =>
        BufferDouble(b.toArray)
          .toSegment(name = path)(using tsc)
          .map(s => Column.F64Column(Vector(s)))
      case (_: Int, b: BufferOf[Int]) =>
        BufferIntInArray(b.toArray)
          .toSegment(name = path)(using tsc)
          .map(s => Column.Int32Column(Vector(s)))
    }

  transparent inline def closeBuffers[T <: Tuple](
      buffers: M[T],
      tsc: TaskSystemComponents,
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
  )(implicit tsc: TaskSystemComponents) = {
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
      .map { v =>
        val columns = v.transpose.map { column =>
          val head = column.head
          val tag = head.tag
          val segments = column
            .flatMap(_.segments)
            .map((_: Segment).asInstanceOf[tag.SegmentType])
          tag.makeTaggedColumn(tag.makeColumn(segments))
        }
      val names = columns.zipWithIndex.map(v => s"${v._2}")
      Table(columns, names, uniqueId, None).as[N, Tuple.Map[T, DefToColumnType]]
      }

  }
}
