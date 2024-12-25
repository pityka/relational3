package ra3

import scala.reflect.ClassTag

case class BufferedTable(
    columns: Vector[Buffer],
    colNames: Vector[String]
) {

  transparent inline def toTuples[T <: Tuple] = {
    val numRows = columns.head.length
    val iter = (0 until numRows).iterator.map { idx =>
      TupleUtils
        .buildTuple[T](0, (i: Int) => columns(i).element(idx))
        .asInstanceOf[T]

    }
    fs2.Stream.fromIterator[cats.effect.IO](iter, 1024)
  }

  private[ra3] transparent inline def toChunkedStream[T]: TupleUtils.M3[T] = {
    assert(columns.size == 1)

    TupleUtils.castArrayToElem[T](columns.head.toArray.asInstanceOf[Array[Any]])

  }

  private[ra3] transparent inline def toTuplesFromColumnChunks[
      N <: Tuple,
      T <: Tuple
  ] = {
    assert(scala.compiletime.constValue[Tuple.Size[T]] == columns.size)

    val numRows = columns.head.length
    val iter = (0 until numRows).iterator.map { idx =>
      scala.NamedTuple.apply[N, ra3.M2[T]](
        TupleUtils
          .buildTupleFromElements[T](0, (i: Int) => columns(i).element(idx))
      )

    }
    fs2.Stream.fromIterator[cats.effect.IO](iter, 1024)

  }

  def toStringFrame(nrows: Int = Int.MaxValue) = {
    import org.saddle.*
    Frame(columns.map { buffer =>
      buffer match {
        case buffer: BufferInstant =>
          buffer.toSeq
            .take(nrows)
            .map(v => java.time.Instant.ofEpochMilli(v).toString)
            .toVec
        case _ => {
          val ar = Array.ofDim[String](math.min(buffer.length, nrows))
          var i = 0
          val n = ar.length
          while (i < n) {
            if (!buffer.isMissing(i)) {
              ar(i) = buffer.elementAsCharSequence(i).toString
            }
            i += 1
          }
          ar.toVec
        }
      }
    }*)
      .setColIndex(colNames.toIndex)
  }

}
