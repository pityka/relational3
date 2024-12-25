package ra3

import scala.reflect.ClassTag
import cats.effect.IO

private object TupleUtils {

  type M[T <: Tuple] <: Tuple = T match {
    case EmptyTuple => EmptyTuple
    case h *: t     => h *: M[t]

  }


  inline def buildTupleFromElements[T <: Tuple](
      inline i: Int,
      inline get: Int => Any
  ): M2[T] = {
    inline scala.compiletime.erasedValue[T] match {
      case _: EmptyTuple => EmptyTuple
      case _: (StrVar *: t) =>
        (get(i)
          .asInstanceOf[CharSequence]) *: buildTupleFromElements[t](i + 1, get)
      case _: (I32Var *: t) =>
        (get(i).asInstanceOf[Int]) *: buildTupleFromElements[t](i + 1, get)
      case _: (I64Var *: t) =>
        (get(i).asInstanceOf[Long]) *: buildTupleFromElements[t](i + 1, get)
      case _: (F64Var *: t) =>
        (get(i).asInstanceOf[Double]) *: buildTupleFromElements[t](i + 1, get)
      case _: (InstVar *: t) =>
        (get(i).asInstanceOf[Long]) *: buildTupleFromElements[t](i + 1, get)
    }
  }
  type Buffers = BufferInt | BufferDouble | BufferLong | BufferString |
    BufferInstant
  type M3[T] = T match {

    case ra3.I32Var *: EmptyTuple  => fs2.Stream[IO, Int]
    case ra3.I64Var *: EmptyTuple  => fs2.Stream[IO, Long]
    case ra3.F64Var *: EmptyTuple  => fs2.Stream[IO, Double]
    case ra3.StrVar *: EmptyTuple  => fs2.Stream[IO, CharSequence]
    case ra3.InstVar *: EmptyTuple => fs2.Stream[IO, Long]

  }

  transparent inline def castArrayToElem[T](inline array: Array[Any]): M3[T] = {
    val t = array
    inline scala.compiletime.erasedValue[T] match {

      case _: (ra3.I32Var *: EmptyTuple) =>
        fs2.Stream.chunk[IO, Int](
          fs2.Chunk.array(
            t.asInstanceOf[Array[Int]]
          )
        )

      case _: (ra3.I64Var *: EmptyTuple) =>
        fs2.Stream.chunk[IO, Long](
          fs2.Chunk.array(
            t.asInstanceOf[Array[Long]]
          )
        )
      case _: (ra3.F64Var *: EmptyTuple) =>
        fs2.Stream.chunk[IO, Double](
          fs2.Chunk.array(
            t.asInstanceOf[Array[Double]]
          )
        )
      case _: (ra3.StrVar *: EmptyTuple) =>
        fs2.Stream.chunk[IO, CharSequence](
          fs2.Chunk.array(
            t.asInstanceOf[Array[CharSequence]]
          )
        )
      case _: (ra3.InstVar *: EmptyTuple) =>
        fs2.Stream.chunk[IO, Long](
          fs2.Chunk.array(
            t.asInstanceOf[Array[Long]]
          )
        )

    }
  }
  inline def buildTuple[T <: Tuple](
      inline i: Int,
      inline get: Int => Any
  ): M[T] = {
    inline scala.compiletime.erasedValue[T] match {
      case _: EmptyTuple => EmptyTuple
      case _: (h *: t) =>
        get(i).asInstanceOf[h] *: buildTuple[t](i + 1, get)
    }
  }

}
