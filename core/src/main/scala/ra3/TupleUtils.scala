package ra3

import scala.reflect.ClassTag
import cats.effect.IO

private object TupleUtils {

  type M[T <: Tuple] <: Tuple = T match {
    case EmptyTuple => EmptyTuple
    case h *: t     => h *: M[t]

  }
  type Elem[T] = T match {
    case ra3.BufferInt     => Int
    case ra3.BufferLong    => Long
    case ra3.BufferDouble  => Double
    case ra3.BufferString  => CharSequence
    case ra3.BufferInstant => Long
  }
  
  inline def elem[T](t: Any): Elem[T] =
    inline scala.compiletime.erasedValue[T] match {
      case _: ra3.BufferInt     => t.asInstanceOf[Int]
      case _: ra3.BufferLong    => t.asInstanceOf[Long]
      case _: ra3.BufferDouble  => t.asInstanceOf[Double]
      case _: ra3.BufferString  => t.asInstanceOf[CharSequence]
      case _: ra3.BufferInstant => t.asInstanceOf[Long]
    }
  type M2[T <: Tuple] <: Tuple = T match {
    case EmptyTuple        => EmptyTuple
    case Either[h, ?] *: t => Elem[h] *: M2[t]

  }
  inline def buildTupleFromElements[T <: Tuple](
      inline i: Int,
      inline get: Int => Any
  ): M2[T] = {
    inline scala.compiletime.erasedValue[T] match {
      case _: EmptyTuple => EmptyTuple
      case _: (Either[h, ?] *: t) =>
        elem[h](get(i)) *: buildTupleFromElements[t](i + 1, get)
    }
  }
  type Buffers = BufferInt | BufferDouble | BufferLong | BufferString |
    BufferInstant
  type M3[T] = T match {
    case Either[h, ?] =>
      h match {
        case ra3.BufferInt     => fs2.Stream[IO, Int]
        case ra3.BufferLong    => fs2.Stream[IO, Long]
        case ra3.BufferDouble  => fs2.Stream[IO, Double]
        case ra3.BufferString  => fs2.Stream[IO, CharSequence]
        case ra3.BufferInstant => fs2.Stream[IO, Long]
      }

  }

  transparent inline def castArrayToElem[T](inline array: Array[Any]): M3[T] = {
    val t = array
    inline scala.compiletime.erasedValue[T] match {
      case _: (Either[h, ?]) =>
        inline scala.compiletime.erasedValue[h] match {
          case _: ra3.BufferInt =>
            fs2.Stream.chunk[IO, Int](
              fs2.Chunk.array(
                t.asInstanceOf[Array[Int]]
              )
            )

          case _: ra3.BufferLong =>
            fs2.Stream.chunk[IO, Long](
              fs2.Chunk.array(
                t.asInstanceOf[Array[Long]]
              )
            )
          case _: ra3.BufferDouble =>
            fs2.Stream.chunk[IO, Double](
              fs2.Chunk.array(
                t.asInstanceOf[Array[Double]]
              )
            )
          case _: ra3.BufferString =>
            fs2.Stream.chunk[IO, CharSequence](
              fs2.Chunk.array(
                t.asInstanceOf[Array[CharSequence]]
              )
            )
          case _: ra3.BufferInstant =>
            fs2.Stream.chunk[IO, Long](
              fs2.Chunk.array(
                t.asInstanceOf[Array[Long]]
              )
            )
        }
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