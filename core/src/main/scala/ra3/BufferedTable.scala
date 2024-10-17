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

  private[ra3] transparent inline def toTuplesFromColumnChunks[T <: Tuple] = {
    assert(scala.compiletime.constValue[Tuple.Size[T]] == columns.size)

    val numRows = columns.head.length
    val iter = (0 until numRows).iterator.map { idx =>
      TupleUtils
        .buildTupleFromElements[T](0, (i: Int) => columns(i).element(idx))

    }
    fs2.Stream.fromIterator[cats.effect.IO](iter, 1024)

  }
  // def toFrame = toStringFrame
  // def toHomogeneousFrameWithRowIndex[Rx, V](rowIndexCol: Int)(implicit
  //     st: org.saddle.ST[V],
  //     st2: org.saddle.ST[Rx],
  //     ord: org.saddle.ORD[Rx]
  // ) = {
  //   import org.saddle.*
  //   val header = columns(rowIndexCol).toSeq.asInstanceOf[Seq[Rx]].toVec

  //   Frame(
  //     columns.zipWithIndex
  //       .filterNot(v => v._2 != rowIndexCol)
  //       .map(_._1)
  //       .map { col =>
  //         col.toSeq.asInstanceOf[Seq[V]].toVec
  //       }*
  //   ).setRowIndex(Index(header.toArray))

  // }

  // def toHomogeneousFrame(
  //     tag: ColumnTag
  // )(implicit st: org.saddle.ST[tag.Elem]) = {
  //   import org.saddle.*
  //   Frame(columns.map(_.toSeq.asInstanceOf[Seq[tag.Elem]].toVec)*)
  //     .setColIndex(colNames.toIndex)
  // }
  def toStringFrame = {
    import org.saddle.*
    Frame(columns.map { buffer =>
      buffer match {
        case buffer: BufferInstant =>
          buffer
            // .as(ra3.ColumnTag.Instant)
            .toSeq
            .map(v => java.time.Instant.ofEpochMilli(v).toString)
            .toVec
        case _ => {
          val ar = Array.ofDim[String](buffer.length)
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
